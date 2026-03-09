"""
Kraken Exchange Integration.

Provides exchange operations for the arbitrage cycle:
  - HMAC-SHA512 authenticated private API calls
  - Market buy orders with fill detection (dual-method)
  - BTC withdrawal to hot wallet
  - Balance queries, trade history, ledger
"""
import base64, hashlib, hmac, logging, time, urllib.parse
import requests
from exchanges.base import ExchangeBase, BuyResult, WithdrawalResult
log = logging.getLogger("bot.kraken")


class KrakenExchange(ExchangeBase):
    name = "kraken"

    def __init__(self, config):
        self.api_key = config.get("api_key", "")
        self.api_secret = config.get("api_secret", "")
        self.trading_pair = config.get("trading_pair", "XXBTZEUR")
        self.base_url = "https://api.kraken.com"
        self.max_buy_fiat = config.get("max_buy_fiat", 2500.0)
        self.withdrawal_key = config.get("withdrawal_key", "")
        self.session = requests.Session()

    def _sign(self, url_path, data):
        """Kraken API signature: HMAC-SHA512(path + SHA256(nonce + postdata))."""
        nonce = str(int(time.time() * 1000))
        data["nonce"] = nonce
        postdata = urllib.parse.urlencode(data)
        encoded = (nonce + postdata).encode()
        message = url_path.encode() + hashlib.sha256(encoded).digest()
        secret = base64.b64decode(self.api_secret)
        mac = hmac.new(secret, message, hashlib.sha512)
        return {
            "API-Key": self.api_key,
            "API-Sign": base64.b64encode(mac.digest()).decode(),
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8"
        }

    def _public(self, endpoint, params=None):
        r = self.session.get(f"{self.base_url}/0/public/{endpoint}",
            params=params or {}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            raise KrakenError(data["error"])
        return data.get("result", {})

    def _private(self, endpoint, data=None):
        url_path = f"/0/private/{endpoint}"
        data = data or {}
        headers = self._sign(url_path, data)
        r = self.session.post(f"{self.base_url}{url_path}",
            data=data, headers=headers, timeout=15)
        r.raise_for_status()
        result = r.json()
        if result.get("error"):
            raise KrakenError(result["error"])
        return result.get("result", {})

    # --- Market Data ---

    def get_spot_price(self, pair=None):
        pair = pair or self.trading_pair
        ticker = self._public("Ticker", {"pair": pair})
        for key, data in ticker.items():
            return float(data["c"][0])
        raise ValueError(f"No ticker for {pair}")

    # --- Account ---

    def get_fiat_balance(self):
        b = self._private("Balance")
        pair = self.trading_pair.upper()
        if "CHF" in pair:
            return float(b.get("ZCHF", b.get("CHF", "0")))
        elif "USD" in pair:
            return float(b.get("ZUSD", b.get("USD", "0")))
        return float(b.get("ZEUR", b.get("EUR", "0")))

    def get_fiat_currency(self):
        pair = self.trading_pair.upper()
        if "CHF" in pair: return "CHF"
        elif "USD" in pair: return "USD"
        return "EUR"

    def get_btc_balance(self):
        b = self._private("Balance")
        return float(b.get("XXBT", b.get("XBT", "0")))

    # --- Trading ---

    def buy_btc_market(self, amount_fiat):
        """Market buy BTC for given fiat amount.
        Uses viqc flag (volume in quote currency).
        Dual fill detection: QueryOrders + balance change fallback."""
        if amount_fiat > self.max_buy_fiat:
            raise ValueError(f"{amount_fiat} exceeds max {self.max_buy_fiat}")

        btc_before = 0
        try:
            btc_before = self.get_btc_balance()
        except Exception:
            pass

        result = self._private("AddOrder", {
            "pair": self.trading_pair, "type": "buy",
            "ordertype": "market", "volume": str(amount_fiat),
            "oflags": "viqc"
        })
        txids = result.get("txid", [])
        if not txids:
            raise KrakenError(f"No order ID: {result}")

        order = self._wait_for_fill(txids[0], btc_before=btc_before)
        return BuyResult(
            order_id=txids[0],
            btc_amount=float(order.get("vol_exec", "0")),
            fiat_spent=float(order.get("cost", "0")),
            fee_fiat=float(order.get("fee", "0")),
            effective_price=float(order.get("price", "0"))
        )

    def withdraw_btc(self, address, amount_btc):
        """Withdraw BTC. Uses withdrawal_key (whitelisted address) if configured."""
        data = {"asset": "XBT", "amount": f"{amount_btc:.8f}"}
        if self.withdrawal_key:
            data["key"] = self.withdrawal_key
        else:
            data["address"] = address
        result = self._private("Withdraw", data)
        return WithdrawalResult(
            withdrawal_id=result.get("refid", ""),
            btc_amount=amount_btc,
            destination=address,
            status="initiated"
        )

    def buy_and_withdraw(self, amount_fiat, address):
        """Buy BTC and withdraw to address in one step."""
        buy = self.buy_btc_market(amount_fiat)
        time.sleep(2)
        withdrawal = self.withdraw_btc(address, buy.btc_amount)
        return {
            "order_id": buy.order_id,
            "total_btc": buy.btc_amount,
            "total_fiat": buy.fiat_spent,
            "fee": buy.fee_fiat,
            "withdrawal_id": withdrawal.withdrawal_id
        }

    # --- Fill Detection ---

    def _wait_for_fill(self, txid, timeout=120, btc_before=None):
        """Wait for order fill with dual detection strategy:
        1. Primary: QueryOrders polling
        2. Fallback (after 15s): balance change detection + TradesHistory
        """
        start = time.time()
        while time.time() - start < timeout:
            elapsed = int(time.time() - start)
            try:
                result = self._private("QueryOrders", {"txid": txid})
                order = result.get(txid, {})
                status = order.get("status", "")
                if status == "closed":
                    return order
                if status in ("canceled", "expired"):
                    raise KrakenError(f"Order {status}")
            except KrakenError:
                raise
            except Exception:
                pass

            # Balance change fallback after 15s
            if elapsed >= 15 and btc_before is not None:
                try:
                    btc_now = self.get_btc_balance()
                    if btc_now > btc_before + 0.000001:
                        trades = self.get_trade_history(days=1)
                        if trades:
                            t = trades[0]
                            return {
                                "status": "closed",
                                "vol_exec": str(t["vol"]),
                                "cost": str(t["cost"]),
                                "fee": str(t["fee"]),
                                "price": str(t["price"])
                            }
                except Exception:
                    pass

            time.sleep(5)
        raise TimeoutError(f"Order {txid} not filled in {timeout}s")

    # --- History ---

    def get_trade_history(self, days=30):
        data = {"start": str(int(time.time()) - days * 86400)}
        result = self._private("TradesHistory", data)
        entries = []
        for tid, t in result.get("trades", {}).items():
            entries.append({
                "id": tid, "pair": t.get("pair", ""),
                "type": t.get("type", ""),
                "price": float(t.get("price", 0)),
                "vol": float(t.get("vol", 0)),
                "cost": float(t.get("cost", 0)),
                "fee": float(t.get("fee", 0)),
                "time": t.get("time", 0),
            })
        return sorted(entries, key=lambda x: x["time"], reverse=True)

    def get_status(self):
        try:
            fiat = self.get_fiat_balance()
            btc = self.get_btc_balance()
            currency = self.get_fiat_currency()
            return {"name": self.name, "online": True,
                    "fiat_balance": fiat, "btc_balance": btc, "currency": currency}
        except Exception as e:
            return {"name": self.name, "online": False, "error": str(e)}


class KrakenError(Exception):
    pass
