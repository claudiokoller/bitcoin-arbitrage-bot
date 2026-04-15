import base64, hashlib, hmac, logging, time, urllib.parse
import requests
from exchanges.base import ExchangeBase, BuyResult, SellResult, WithdrawalResult
log = logging.getLogger("bot.kraken")

class KrakenExchange(ExchangeBase):
    def __init__(self, config):
        self.name = config.get("name", "kraken")
        self.api_key = config.get("api_key","")
        self.api_secret = config.get("api_secret","")
        self.trading_pair = config.get("trading_pair","XXBTZEUR")
        self.base_url = "https://api.kraken.com"
        self.max_buy_fiat = config.get("max_buy_fiat",2500.0)
        self.withdrawal_key = config.get("withdrawal_key","")
        self.session = requests.Session()
        self._balance_cache = {}   # {"fiat": (value, ts), "btc": (value, ts)}
        self._balance_cache_ttl = 60  # seconds
    def _sign(self, url_path, data):
        nonce = str(int(time.time()*1000))
        data["nonce"] = nonce
        postdata = urllib.parse.urlencode(data)
        encoded = (nonce+postdata).encode()
        message = url_path.encode() + hashlib.sha256(encoded).digest()
        secret = base64.b64decode(self.api_secret)
        mac = hmac.new(secret, message, hashlib.sha512)
        return {"API-Key":self.api_key,"API-Sign":base64.b64encode(mac.digest()).decode(),"Content-Type":"application/x-www-form-urlencoded; charset=utf-8"}
    def _public(self, endpoint, params=None):
        r = self.session.get(f"{self.base_url}/0/public/{endpoint}", params=params or {}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("error"): raise KrakenError(data["error"])
        return data.get("result",{})
    def _private(self, endpoint, data=None):
        url_path = f"/0/private/{endpoint}"
        data = data or {}
        headers = self._sign(url_path, data)
        r = self.session.post(f"{self.base_url}{url_path}", data=data, headers=headers, timeout=15)
        r.raise_for_status()
        result = r.json()
        if result.get("error"): raise KrakenError(result["error"])
        return result.get("result",{})
    def get_spot_price(self, pair=None):
        pair = pair or self.trading_pair
        ticker = self._public("Ticker", {"pair":pair})
        for key, data in ticker.items(): return float(data["c"][0])
        raise ValueError(f"No ticker for {pair}")
    def _invalidate_balance_cache(self):
        self._balance_cache.clear()

    def get_fiat_balance(self, cached=False):
        if cached:
            entry = self._balance_cache.get("fiat")
            if entry and time.time() - entry[1] < self._balance_cache_ttl:
                return entry[0]
        b = self._private("Balance")
        pair = self.trading_pair.upper()
        if "CHF" in pair:
            val = float(b.get("ZCHF", b.get("CHF", "0")))
        elif "USD" in pair:
            val = float(b.get("ZUSD", b.get("USD", "0")))
        else:
            val = float(b.get("ZEUR", b.get("EUR", "0")))
        self._balance_cache["fiat"] = (val, time.time())
        return val

    def get_fiat_currency(self):
        pair = self.trading_pair.upper()
        if "CHF" in pair: return "CHF"
        elif "USD" in pair: return "USD"
        else: return "EUR"
    def get_btc_balance(self, cached=False):
        if cached:
            entry = self._balance_cache.get("btc")
            if entry and time.time() - entry[1] < self._balance_cache_ttl:
                return entry[0]
        b = self._private("Balance")
        val = float(b.get("XXBT", b.get("XBT", "0")))
        self._balance_cache["btc"] = (val, time.time())
        return val
    def buy_btc_market(self, amount_fiat):
        if amount_fiat > self.max_buy_fiat: raise ValueError(f"{amount_fiat} exceeds {self.max_buy_fiat}")
        currency = self.get_fiat_currency()
        log.info(f"Market buy BTC for {amount_fiat:.2f} {currency}")
        # Capture balance BEFORE order for fill detection
        btc_before = 0
        try:
            btc_before = self.get_btc_balance()
        except Exception as e:
            log.debug(f"Pre-order balance check: {e}")
        result = self._private("AddOrder", {"pair":self.trading_pair,"type":"buy","ordertype":"market","volume":str(amount_fiat),"oflags":"viqc"})
        txids = result.get("txid",[])
        if not txids: raise KrakenError(f"No order ID returned: {result}")
        log.info(f"Order placed: {txids[0]}")
        order = self._wait_for_fill(txids[0], btc_before=btc_before)
        btc = float(order.get("vol_exec","0"))
        cost = float(order.get("cost","0"))
        fee = float(order.get("fee","0"))
        price = float(order.get("price","0"))
        log.info(f"FILLED: {btc:.8f} BTC for {cost:.2f} {currency} (fee: {fee:.4f}, price: {price:.2f})")
        self._invalidate_balance_cache()  # force fresh balance after buy
        return BuyResult(order_id=txids[0], btc_amount=btc, fiat_spent=cost, fee_fiat=fee, effective_price=price)
    def sell_btc_market(self, amount_btc):
        currency = self.get_fiat_currency()
        log.info(f"Market sell {amount_btc:.8f} BTC for {currency}")
        result = self._private("AddOrder", {"pair": self.trading_pair, "type": "sell", "ordertype": "market", "volume": f"{amount_btc:.8f}"})
        txids = result.get("txid", [])
        if not txids: raise KrakenError(f"No order ID returned: {result}")
        log.info(f"Sell order placed: {txids[0]}")
        order = self._wait_for_fill(txids[0])
        btc = float(order.get("vol_exec", "0"))
        cost = float(order.get("cost", "0"))
        fee = float(order.get("fee", "0"))
        price = float(order.get("price", "0"))
        log.info(f"SOLD: {btc:.8f} BTC for {cost:.2f} {currency} (fee: {fee:.4f}, price: {price:.2f})")
        return SellResult(order_id=txids[0], btc_sold=btc, fiat_received=cost, fee_fiat=fee, effective_price=price)

    def withdraw_btc(self, address, amount_btc):
        log.info(f"Withdraw {float(amount_btc):.8f} BTC -> {address[:20]}...")
        data = {"asset":"XBT","amount":f"{amount_btc:.8f}"}
        if self.withdrawal_key: data["key"] = self.withdrawal_key
        else: data["address"] = address
        result = self._private("Withdraw", data)
        return WithdrawalResult(withdrawal_id=result.get("refid",""), btc_amount=amount_btc, destination=address, status="initiated")
    def _wait_for_fill(self, txid, timeout=120, btc_before=None):
        start = time.time()
        status = "pending"

        while time.time()-start < timeout:
            elapsed = int(time.time()-start)

            # Method 1: QueryOrders
            try:
                result = self._private("QueryOrders", {"txid":txid})
                order = result.get(txid,{})
                status = order.get("status","")
                if status == "closed":
                    log.info(f"Order filled after {elapsed}s")
                    return order
                if status in ("canceled","expired"): raise KrakenError(f"Order {status}")
            except KrakenError:
                raise
            except Exception as e:
                log.debug(f"QueryOrders {txid}: {e}")

            # Method 2: After 15s, check balance change as fallback
            if elapsed >= 15 and btc_before is not None:
                try:
                    btc_now = self.get_btc_balance()  # always fresh here (fill detection)
                    if btc_now > btc_before + 0.000001:
                        diff = btc_now - btc_before
                        log.info(f"Balance changed! +{diff:.8f} BTC — Order filled after {elapsed}s")
                        # Get trade details from TradesHistory
                        try:
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
                        except Exception as e:
                            log.debug(f"TradesHistory fallback: {e}")
                        # Fallback: estimate cost from spot price
                        try:
                            spot = self.get_spot_price()
                            est_cost = diff * spot
                        except Exception:
                            spot, est_cost = 0, 0
                        log.warning(f"Using balance-diff fallback: {diff:.8f} BTC, est. cost {est_cost:.2f}")
                        return {
                            "status": "closed",
                            "vol_exec": str(diff),
                            "cost": str(round(est_cost, 2)), "fee": "0", "price": str(round(spot, 2))
                        }
                except Exception as e:
                    log.debug(f"Balance check fallback: {e}")

            log.debug(f"Waiting for fill... ({elapsed}s, status: {status or 'pending'})")
            time.sleep(5)
        raise TimeoutError(f"Order {txid} not filled in {timeout}s")


    def get_status(self):
        try:
            fiat = self.get_fiat_balance()
            btc = self.get_btc_balance()
            currency = self.get_fiat_currency()
            return {"name": self.name, "online": True, "fiat_balance": fiat, "btc_balance": btc, "currency": currency}
        except Exception as e:
            return {"name": self.name, "online": False, "error": str(e)}

    def buy_and_withdraw(self, amount_fiat, address):
        """Buy BTC and withdraw to address (Hot Wallet)"""
        buy = self.buy_btc_market(amount_fiat)
        # Small delay for Kraken to settle
        time.sleep(2)
        withdrawal = self.withdraw_btc(address, buy.btc_amount)
        return {"order_id": buy.order_id, "total_btc": buy.btc_amount, "total_fiat": buy.fiat_spent, "fee": buy.fee_fiat, "withdrawal_id": withdrawal.withdrawal_id}

    def get_ledger(self, asset=None, type=None, days=30):
        """Get ledger entries (deposits, withdrawals, trades)"""
        data = {"ofs": 0}
        if asset: data["asset"] = asset
        if type: data["type"] = type
        data["start"] = str(int(time.time()) - days * 86400)
        result = self._private("Ledger", data)
        ledger = result.get("ledger", {})
        entries = []
        for lid, entry in ledger.items():
            entries.append({
                "id": lid,
                "type": entry.get("type", ""),
                "asset": entry.get("asset", ""),
                "amount": float(entry.get("amount", 0)),
                "fee": float(entry.get("fee", 0)),
                "balance": float(entry.get("balance", 0)),
                "time": entry.get("time", 0),
            })
        return sorted(entries, key=lambda x: x["time"], reverse=True)

    def get_trade_history(self, days=30):
        """Get closed trades"""
        data = {"start": str(int(time.time()) - days * 86400)}
        result = self._private("TradesHistory", data)
        trades = result.get("trades", {})
        entries = []
        for tid, t in trades.items():
            entries.append({
                "id": tid,
                "pair": t.get("pair", ""),
                "type": t.get("type", ""),
                "price": float(t.get("price", 0)),
                "vol": float(t.get("vol", 0)),
                "cost": float(t.get("cost", 0)),
                "fee": float(t.get("fee", 0)),
                "time": t.get("time", 0),
            })
        return sorted(entries, key=lambda x: x["time"], reverse=True)

class KrakenError(Exception): pass
