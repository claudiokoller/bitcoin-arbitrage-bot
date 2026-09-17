import hashlib, hmac, json, logging, time
import requests
from exchanges.base import ExchangeBase, BuyResult, WithdrawalResult

log = logging.getLogger("bot.bitvavo")


class BitvavoError(Exception):
    pass


class BitvavoExchange(ExchangeBase):
    """Bitvavo (api.bitvavo.com/v2). Chosen 2026-06-29 to replace Binance (SEPA-Stop):
    network-based BTC withdrawal fee (~1-3 EUR, not a flat 0.0005 BTC), 0.25% taker,
    free SEPA, CH in SEPA zone. Same interface as BinanceExchange.

    Auth: HMAC-SHA256 over (timestamp + method + '/v2' + endpoint[+query] + body).
    Market-Buy is sized directly in EUR via amountQuote.
    NOTE: withdrawal address must be whitelisted in the Bitvavo address book first.
    """
    BASE_URL = "https://api.bitvavo.com/v2"

    def __init__(self, config):
        self.name              = config.get("name", "Bitvavo")
        self.api_key           = config.get("api_key", "")
        self.api_secret        = config.get("api_secret", "")
        self.trading_pair      = config.get("trading_pair", "BTC-EUR")  # Bitvavo market format
        self.max_buy_fiat      = config.get("max_buy_fiat", 2500.0)
        self.withdrawal_label   = config.get("withdrawal_label", "HotWallet")
        self.withdrawal_address = config.get("withdrawal_address", "")  # hot wallet BTC address
        self.access_window     = str(config.get("access_window_ms", 10000))
        self.session           = requests.Session()
        self._balance_cache    = {}
        self._balance_cache_ttl = 60
        self._withdrawal_fee_cache = None  # (sats, ts)

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _headers(self, method, endpoint, body=""):
        ts = str(int(time.time() * 1000))
        msg = ts + method.upper() + "/v2" + endpoint + body
        sig = hmac.new(self.api_secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
        return {
            "Bitvavo-Access-Key":       self.api_key,
            "Bitvavo-Access-Signature": sig,
            "Bitvavo-Access-Timestamp": ts,
            "Bitvavo-Access-Window":    self.access_window,
            "Content-Type":             "application/json",
        }

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    def _public(self, endpoint, params=None):
        r = self.session.get(f"{self.BASE_URL}{endpoint}", params=params or {}, timeout=15)
        r.raise_for_status()
        return r.json()

    def _private_get(self, endpoint, params=None):
        # Query string must be identical in the signed message and the request URL.
        if params:
            from urllib.parse import urlencode
            endpoint = f"{endpoint}?{urlencode(params)}"
        r = self.session.get(f"{self.BASE_URL}{endpoint}",
                             headers=self._headers("GET", endpoint), timeout=15)
        return self._handle(r, endpoint)

    def _private_post(self, endpoint, payload):
        body = json.dumps(payload, separators=(",", ":"))
        r = self.session.post(f"{self.BASE_URL}{endpoint}",
                              headers=self._headers("POST", endpoint, body), data=body, timeout=15)
        return self._handle(r, endpoint)

    def _handle(self, r, endpoint):
        try:
            d = r.json()
        except Exception:
            raise BitvavoError(f"HTTP {r.status_code} {endpoint}: {r.text[:300]}")
        # Bitvavo errors: {"errorCode": 205, "error": "..."}
        if isinstance(d, dict) and d.get("errorCode") is not None:
            raise BitvavoError(f"{d.get('errorCode')} {endpoint}: {d.get('error', '')}")
        if not r.ok:
            raise BitvavoError(f"HTTP {r.status_code} {endpoint}: {r.text[:300]}")
        return d

    # ── Market data ───────────────────────────────────────────────────────────

    def get_spot_price(self, pair=None):
        d = self._public("/ticker/price", {"market": pair or self.trading_pair})
        return float(d["price"])

    def get_fiat_currency(self):
        p = self.trading_pair.upper()
        for cur in ("EUR", "USD", "CHF"):
            if p.endswith(cur):
                return cur
        return "EUR"

    # ── Balances ─────────────────────────────────────────────────────────────

    def _balance_for(self, symbol):
        data = self._private_get("/balance", {"symbol": symbol})
        for b in (data if isinstance(data, list) else []):
            if b.get("symbol") == symbol:
                return float(b.get("available", 0)) + float(b.get("inOrder", 0))
        return 0.0

    def get_fiat_balance(self, cached=False):
        if cached:
            e = self._balance_cache.get("fiat")
            if e and time.time() - e[1] < self._balance_cache_ttl:
                return e[0]
        val = self._balance_for(self.get_fiat_currency())
        self._balance_cache["fiat"] = (val, time.time())
        return val

    def get_btc_balance(self, cached=False):
        if cached:
            e = self._balance_cache.get("btc")
            if e and time.time() - e[1] < self._balance_cache_ttl:
                return e[0]
        val = self._balance_for("BTC")
        self._balance_cache["btc"] = (val, time.time())
        return val

    def _invalidate_balance_cache(self):
        self._balance_cache.clear()

    # ── Withdrawal fee ────────────────────────────────────────────────────────

    def get_withdrawal_fee_sats(self) -> int:
        """BTC withdrawal fee in sats via /v2/assets. Bitvavo's fee is network-based
        (variable). Cached 10 min."""
        if self._withdrawal_fee_cache and time.time() - self._withdrawal_fee_cache[1] < 600:
            return self._withdrawal_fee_cache[0]
        try:
            d = self._public("/assets", {"symbol": "BTC"})
            asset = d[0] if isinstance(d, list) else d
            fee_sats = int(float(asset.get("withdrawalFee", 0.0002)) * 1e8)
            self._withdrawal_fee_cache = (fee_sats, time.time())
            log.info(f"Bitvavo withdrawal fee: {fee_sats:,} sats")
            return fee_sats
        except Exception as e:
            log.warning(f"get_withdrawal_fee_sats: {e} — using default 20000")
        return 20_000  # 0.0002 BTC fallback

    # ── Deposits ──────────────────────────────────────────────────────────────

    def get_btc_deposits(self, start_time_ms=None):
        """BTC deposit history via /v2/depositHistory, normalized to the Binance-style
        shape the engine expects: status '1' = completed, plus txId/amount/id."""
        params = {"symbol": "BTC"}
        if start_time_ms:
            params["start"] = int(start_time_ms)
        d = self._private_get("/depositHistory", params)
        out = []
        for dep in (d if isinstance(d, list) else []):
            status = "1" if str(dep.get("status", "")).lower() == "completed" else "0"
            out.append({
                "amount": dep.get("amount", 0),
                "coin":   dep.get("symbol", "BTC"),
                "txId":   dep.get("txId", ""),
                "id":     dep.get("txId", "") or str(dep.get("timestamp", "")),
                "status": status,
                "insertTime": dep.get("timestamp", 0),
                "address": dep.get("address", ""),
            })
        return out

    def get_fiat_deposits(self, start_time_ms=None):
        """EUR deposit history. Bitvavo exposes fiat deposits via /v2/depositHistory
        with symbol=EUR (no sender IBAN). Returns Binance-fiat-order-like dicts so the
        deposit-notify feature can render amount + method."""
        params = {"symbol": self.get_fiat_currency()}
        if start_time_ms:
            params["start"] = int(start_time_ms)
        try:
            d = self._private_get("/depositHistory", params)
        except Exception as e:
            log.debug(f"Bitvavo get_fiat_deposits: {e}")
            return []
        out = []
        for dep in (d if isinstance(d, list) else []):
            out.append({
                "orderNo":     str(dep.get("timestamp", "")),
                "fiatCurrency": dep.get("symbol", self.get_fiat_currency()),
                "amount":      dep.get("amount", 0),
                "totalFee":    dep.get("fee", 0),
                "method":      "SEPA",
                "status":      "Successful" if str(dep.get("status", "")).lower() == "completed" else "Processing",
                "createTime":  dep.get("timestamp", 0),
            })
        return out

    # ── Trading ───────────────────────────────────────────────────────────────

    def buy_btc_market(self, amount_fiat) -> BuyResult:
        if amount_fiat > self.max_buy_fiat:
            raise ValueError(f"{amount_fiat} exceeds max {self.max_buy_fiat}")
        currency = self.get_fiat_currency()
        log.info(f"Bitvavo: market buy BTC for {amount_fiat:.2f} {currency}")

        # amountQuote = spend exact fiat amount (like Binance quoteOrderQty)
        result = self._private_post("/order", {
            "market":     self.trading_pair,
            "side":       "buy",
            "orderType":  "market",
            "amountQuote": f"{amount_fiat:.2f}",
        })

        btc_amount = float(result.get("filledAmount", 0))
        cost       = float(result.get("filledAmountQuote", amount_fiat))
        spot       = cost / btc_amount if btc_amount else 0

        fee_paid   = float(result.get("feePaid", 0) or 0)
        fee_cur    = result.get("feeCurrency", currency)
        if fee_cur == "BTC":
            fee_fiat = fee_paid * spot
        else:
            fee_fiat = fee_paid  # EUR or quote currency
        if not fee_fiat:
            fee_fiat = cost * 0.0025  # 0.25% taker fallback

        log.info(f"Bitvavo: bought {btc_amount:.8f} BTC for {cost:.2f} {currency} "
                 f"@ {spot:.2f} fee={fee_fiat:.4f}")
        self._invalidate_balance_cache()
        return BuyResult(
            order_id=str(result.get("orderId", "")),
            btc_amount=btc_amount,
            fiat_spent=cost,
            fee_fiat=fee_fiat,
            effective_price=spot,
        )

    # ── Withdrawal ────────────────────────────────────────────────────────────

    def withdraw_btc(self, address, amount_btc) -> WithdrawalResult:
        addr = address or self.withdrawal_address
        if not addr:
            raise ValueError("Bitvavo: no withdrawal address — set 'withdrawal_address' in config")
        log.info(f"Bitvavo: withdraw {amount_btc:.8f} BTC -> {addr[:20]}...")
        # Bitvavo requires the destination to be whitelisted in the address book.
        result = self._private_post("/withdrawal", {
            "symbol":  "BTC",
            "amount":  f"{amount_btc:.8f}",
            "address": addr,
        })
        # /v2/withdrawal returns {"success": true} (no id)
        wid = str(result.get("success", "")) if isinstance(result, dict) else ""
        log.info(f"Bitvavo: withdrawal initiated: {result}")
        self._invalidate_balance_cache()
        return WithdrawalResult(
            withdrawal_id=wid,
            btc_amount=amount_btc,
            destination=addr,
            status="initiated",
        )

    def buy_and_withdraw(self, amount_fiat, address):
        buy = self.buy_btc_market(amount_fiat)
        time.sleep(3)
        withdrawal = self.withdraw_btc(address, buy.btc_amount)
        return {
            "order_id":     buy.order_id,
            "total_btc":    buy.btc_amount,
            "total_fiat":   buy.fiat_spent,
            "fee":          buy.fee_fiat,
            "withdrawal_id": withdrawal.withdrawal_id,
        }
