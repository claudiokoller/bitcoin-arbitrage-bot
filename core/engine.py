import json, logging, os, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests as _requests
from core.models import SellOffer, OfferStatus, Platform, Exchange, TradeResult
from core.trade_logger import TradeLogger
from core.pricing import DynamicPricer
from exchanges.base import ExchangeBase
from platforms.base import PlatformBase
log = logging.getLogger("bot.engine")

# Transient errors worth retrying (network issues, server overload)
_RETRYABLE = (ConnectionError, TimeoutError, OSError,
              _requests.ConnectionError, _requests.Timeout, _requests.HTTPError)

def _is_retryable(exc):
    """Check if an exception is transient (worth retrying) vs fatal (config/logic)."""
    if isinstance(exc, _RETRYABLE):
        # 4xx (except 429) are NOT retryable — bad request, auth, not found
        if isinstance(exc, _requests.HTTPError) and exc.response is not None:
            code = exc.response.status_code
            return code >= 500 or code == 429
        return True
    # String check fallback for wrapped exceptions
    s = str(exc).lower()
    return any(kw in s for kw in ("timeout", "connection", "503", "502", "429", "retry"))

def _mempool_get(url, timeout=10, retries=3):
    """GET from mempool.space with retry + blockstream.info fallback."""
    import requests as _req
    last_err = None
    for attempt in range(retries):
        try:
            r = _req.get(url, timeout=timeout)
            if r.status_code >= 500 and attempt < retries - 1:
                time.sleep(2 ** attempt); continue
            r.raise_for_status()
            return r.json()
        except (_req.ConnectionError, _req.Timeout) as e:
            last_err = e
            if attempt < retries - 1: time.sleep(2 ** attempt)
    # Fallback to blockstream.info
    fallback_url = url.replace("https://mempool.space/api", "https://blockstream.info/api")
    if fallback_url != url:
        try:
            r = _req.get(fallback_url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception:
            pass
    raise last_err or Exception(f"mempool.space + blockstream.info request failed")

class SpotPriceProvider:
    _cache = {}  # {currency: (price, timestamp)} — fallback if all sources fail

    @staticmethod
    def _cached(currency, price):
        SpotPriceProvider._cache[currency] = (price, time.time())
        return price

    @staticmethod
    def _get_cached(currency, max_age=120):
        """Return cached price if fresh enough (default 2min), else None.
        Hard limit: reject cache older than 5min to prevent stale-price trades."""
        HARD_MAX_AGE = 300  # 5 minutes absolute maximum
        entry = SpotPriceProvider._cache.get(currency)
        if not entry:
            return None
        age = time.time() - entry[1]
        if age > HARD_MAX_AGE:
            log.error(f"Cached {currency} spot price too old ({age:.0f}s), rejecting")
            return None
        if age < max_age:
            log.warning(f"Using cached {currency} spot price ({age:.0f}s old)")
            return entry[0]
        return None

    @staticmethod
    def get_spot_eur():
        import requests
        try:
            r = requests.get("https://api.coingecko.com/api/v3/simple/price", params={"ids":"bitcoin","vs_currencies":"eur"}, timeout=10)
            r.raise_for_status()
            return SpotPriceProvider._cached("EUR", r.json()["bitcoin"]["eur"])
        except Exception as e:
            log.debug(f"CoinGecko EUR: {e}")
        try:
            r = requests.get("https://api.kraken.com/0/public/Ticker", params={"pair":"XXBTZEUR"}, timeout=10)
            r.raise_for_status()
            for v in r.json()["result"].values(): return SpotPriceProvider._cached("EUR", float(v["c"][0]))
        except Exception as e:
            log.debug(f"Kraken EUR: {e}")
        cached = SpotPriceProvider._get_cached("EUR")
        if cached: return cached
        raise ValueError("Cannot fetch spot price")

    @staticmethod
    def get_spot_usd():
        import requests
        try:
            r = requests.get("https://api.kraken.com/0/public/Ticker", params={"pair":"XBTUSD"}, timeout=10)
            r.raise_for_status()
            for v in r.json()["result"].values(): return SpotPriceProvider._cached("USD", float(v["c"][0]))
        except Exception as e:
            log.debug(f"Kraken USD: {e}")
        try:
            r = requests.get("https://api.coingecko.com/api/v3/simple/price", params={"ids":"bitcoin","vs_currencies":"usd"}, timeout=10)
            r.raise_for_status()
            return SpotPriceProvider._cached("USD", r.json()["bitcoin"]["usd"])
        except Exception as e:
            log.debug(f"CoinGecko USD: {e}")
        cached = SpotPriceProvider._get_cached("USD")
        if cached: return cached
        raise ValueError("Cannot fetch USD spot price")

    @staticmethod
    def get_spot_chf():
        import requests
        try:
            r = requests.get("https://api.coingecko.com/api/v3/simple/price", params={"ids":"bitcoin","vs_currencies":"chf"}, timeout=10)
            r.raise_for_status()
            return SpotPriceProvider._cached("CHF", r.json()["bitcoin"]["chf"])
        except Exception as e:
            log.debug(f"CoinGecko CHF: {e}")
        try:
            eur = SpotPriceProvider.get_spot_eur()
            r = requests.get("https://api.kraken.com/0/public/Ticker", params={"pair":"EURCHF"}, timeout=10)
            r.raise_for_status()
            for v in r.json()["result"].values(): return SpotPriceProvider._cached("CHF", eur * float(v["c"][0]))
        except Exception as e:
            log.debug(f"Kraken EURCHF: {e}")
        cached = SpotPriceProvider._get_cached("CHF")
        if cached: return cached
        raise ValueError("Cannot fetch CHF spot price")

    @staticmethod
    def get_spot(currency="EUR"):
        if currency == "EUR": return SpotPriceProvider.get_spot_eur()
        elif currency == "USD": return SpotPriceProvider.get_spot_usd()
        elif currency == "CHF": return SpotPriceProvider.get_spot_chf()
        else: return SpotPriceProvider.get_spot_eur()

class TradingEngine:
    def __init__(self, config):
        self.config = config
        self.platforms = {}
        self.exchanges = {}
        self.trade_logger = TradeLogger(config.get("db_path","trades.db"))
        self.pricer = DynamicPricer(config)
        self.notifier = None
        self.pending_escrows = {}
        self._escrow_lock = threading.Lock()  # protects pending_escrows access
        # Share lock with fund_from_wallet to prevent concurrent UTXO spending
        try:
            from fund_from_wallet import set_funding_lock
            set_funding_lock(self._escrow_lock)
        except ImportError:
            pass
        self._contracted_offers = set()  # Offer IDs that have active contracts (don't re-sync, max 100)
        self._contracted_offers_file = os.path.join(os.path.dirname(__file__), "..", "contracted_offers.json")
        self._load_contracted_offers()
        self._contracted_offers_snapshot = set(self._contracted_offers)
        self.daily_volume_sats = 0
        self.paused = False
        self.poll_interval = config.get("poll_interval",30)
        self._poll_active = config.get("poll_interval_active", 12)   # funded offers exist
        self._poll_idle = config.get("poll_interval_idle", 60)       # no funded offers
        self.daily_volume_limit = config.get("daily_volume_limit_usd",2000.0)
        self.auto_fund = config.get("auto_buy_and_fund",False)
        self.auto_confirm = config.get("auto_confirm_payment",False)
        self._notified_contracts = set()  # track already-notified contract IDs
        self._recorded_contracts = set()  # track already-logged trades
        self._low_balance_warned = {}  # {exchange_name: float (timestamp)} — per-exchange last warning time
        self._buy_data_cache = {}  # preserved buy data after offer removed from pending_escrows
        self._start_time = datetime.now()
        self._last_daily_summary = None
        self._last_weekly_summary = None
        self._pending_refunds = {}  # {offer_id: {escrow_addr, amount_sats, created}}
        self._last_refund_check = None
        self._premium_reductions = {}  # {offer_id: original_premium} for tracking reductions
        self._last_stale_check = None
        self._last_consolidation_check = None
        self._last_auto_buy_check = 0
        self._last_auto_fund_wallet_check = 0
        self._last_contracted_offers_save = 0
        self._platform_down_since = {}     # {name: timestamp} — when platform first became unreachable
        self._platform_down_notified = set()  # platforms where outage alert was already sent
        self._escrow_state_file = os.path.join(os.path.dirname(__file__), "..", "escrow_state.json")
        self._escrow_state = self._load_escrow_state()
        self._cached_active_offers = {}  # {platform_name: [offers]} — reused by _auto_broadcast_refunds
    def reload_config(self, config_path=None):
        """Hot-reload config.json — updates engine config, exchanges, and platform credentials."""
        import os as _os
        if config_path is None:
            config_path = _os.path.join(_os.path.dirname(__file__), "..", "config.json")
        with open(config_path) as f:
            new_cfg = json.load(f)
        self.config = new_cfg

        # Reinitialize exchanges with new API keys
        from exchanges.kraken import KrakenExchange
        for key, ecfg in new_cfg.get("exchanges", {}).items():
            if not ecfg.get("enabled"): continue
            if not key.startswith("kraken"): continue
            ecfg.setdefault("name", key)
            ex_name = ecfg["name"]
            try:
                new_ex = KrakenExchange(ecfg)
                self.exchanges[ex_name] = new_ex
                log.info(f"reload_config: exchange {ex_name} reinitialized")
            except Exception as e:
                log.warning(f"reload_config: exchange {ex_name}: {e}")

        # Update Peach platform credentials without full re-init
        peach = self.platforms.get("peach")
        if peach:
            pcfg = new_cfg.get("platforms", {}).get("peach", {})
            for attr in ("payment_data_raw", "payment_data_info", "pgp_private_key",
                         "pgp_public_key", "refund_address"):
                val = pcfg.get(attr)
                if val is not None:
                    setattr(peach, attr, val)
            peach.access_token = None  # force re-auth on next tick
            log.info("reload_config: peach credentials updated, re-auth scheduled")

        log.info("Config reloaded successfully")

    def add_platform(self, p):
        self.platforms[p.name] = p
        log.info(f"Platform: {p.name}")
    def add_exchange(self, e):
        self.exchanges[e.name] = e
        log.info(f"Exchange: {e.name}")
    def set_notifier(self, n): self.notifier = n

    def add_pending_escrow(self, offer_id, escrow_addr, amount_sats, premium, sepa_account_index=0):
        """Register a new offer in pending_escrows and log it. Thread-safe."""
        with self._escrow_lock:
            self.pending_escrows[offer_id] = {
                "platform": "peach", "escrow_address": escrow_addr,
                "amount_sats": amount_sats, "funded": False,
                "funding_in_progress": True, "premium": premium,
                "sepa_account_index": sepa_account_index}
        self.trade_logger.log_event("peach", "offer_created", offer_id)
    def get_best_exchange(self):
        best = None
        best_price = float("inf")
        for ex in self.exchanges.values():
            try:
                price = ex.get_spot_price()
                if price < best_price:
                    best_price = price
                    best = ex
            except Exception as e:
                log.warning(f"{ex.name}: {e}")
        return best
    def get_exchange_by_currency(self, currency):
        """Get exchange instance that trades in the given fiat currency."""
        for ex in self.exchanges.values():
            if hasattr(ex, 'get_fiat_currency') and ex.get_fiat_currency() == currency:
                return ex
        return None
    def start(self):
        log.info("="*50)
        log.info(f"  Platforms: {list(self.platforms.keys())}")
        log.info(f"  Exchanges: {list(self.exchanges.keys())}")
        log.info(f"  Auto-fund: {self.auto_fund}")
        log.info("="*50)
        for name, p in self.platforms.items():
            try:
                p.authenticate()
                log.info(f"{name}: auth OK")
            except Exception as e:
                log.error(f"{name}: auth failed: {e}")
        for name, e in self.exchanges.items():
            try:
                s = e.get_status()
                currency = s.get('currency', 'CHF')
                log.info(f"{name}: {s.get('fiat_balance',0):.2f} {currency}")
            except Exception as e2:
                log.warning(f"{name}: {e2}")
        self._load_pending_refunds()
        _consecutive_errors = 0
        while True:
            try:
                if not self.paused: self._tick()
                _consecutive_errors = 0
            except KeyboardInterrupt:
                log.info("Shutting down...")
                break
            except Exception as e:
                _consecutive_errors += 1
                if _is_retryable(e):
                    log.warning(f"Tick transient error ({_consecutive_errors}x): {e}")
                else:
                    log.error(f"Tick fatal error: {e}")
                    if self.notifier: self.notifier.notify_error(str(e))
                # Back off on repeated errors: 1x=normal, 2x=double, max 5min
                if _consecutive_errors >= 3:
                    backoff = min(self.poll_interval * _consecutive_errors, 300)
                    log.warning(f"Backing off {backoff}s after {_consecutive_errors} consecutive errors")
                    time.sleep(backoff)
                    continue
            # Adaptive polling: faster when funded offers exist (waiting for matches)
            with self._escrow_lock:
                has_funded = any(v.get("funded") for v in self.pending_escrows.values())
            interval = self._poll_active if has_funded else self._poll_idle
            time.sleep(interval)
    def _prune_tracking_sets(self):
        """Prevent unbounded growth of tracking sets."""
        for s, label in ((self._notified_contracts, "_notified_contracts"),
                         (self._recorded_contracts, "_recorded_contracts")):
            if len(s) > 200:
                # Keep newest 100 (contract IDs are roughly chronological)
                sorted_ids = sorted(s)
                to_remove = sorted_ids[:-100]
                s -= set(to_remove)
                log.debug(f"Pruned {label}: {len(to_remove)} old entries removed")

    def _tick(self):
        tick_start = time.time()
        for name, platform in self.platforms.items():
            try: self._process_platform(name, platform)
            except Exception as e: log.error(f"{name}: {e}")
        self._check_daily_summary()
        self._check_weekly_summary()
        self._check_low_balance()
        self._check_pending_refunds()
        self._auto_consolidate_utxos()
        self._auto_buy_escrow_check()
        self._auto_fund_wallet_check()
        self._prune_tracking_sets()
        tick_duration = time.time() - tick_start
        if tick_duration > self.poll_interval:
            log.warning(f"Tick took {tick_duration:.1f}s (> {self.poll_interval}s poll interval)")
    def _check_daily_summary(self):
        now = datetime.now()
        if now.hour == 22 and (self._last_daily_summary is None or self._last_daily_summary.date() < now.date()):
            self._last_daily_summary = now
            if self.notifier:
                try:
                    s = self.trade_logger.get_daily_summary()
                    self.notifier.notify_daily_summary(s)
                except Exception as e:
                    log.debug(f"Daily summary: {e}")

    def _check_weekly_summary(self):
        now = datetime.now()
        if now.weekday() != 6 or now.hour != 22: return  # Sonntag 22:00
        week = now.isocalendar()[1]
        if self._last_weekly_summary and self._last_weekly_summary.isocalendar()[1] == week:
            return
        self._last_weekly_summary = now
        if self.notifier:
            try:
                w = self.trade_logger.get_period_summary(days=7)
                b = self.trade_logger.get_platform_breakdown(days=7)
                self.notifier.notify_weekly_summary(w, b)
            except Exception as e:
                log.debug(f"Weekly summary: {e}")
    def _check_low_balance(self):
        now = datetime.now()
        if now.minute not in (0, 1): return  # check at top of hour (tolerant of 30s tick)
        threshold = self.config.get("low_balance_threshold", 50)
        cooldown = self.config.get("low_balance_cooldown_sec", 21600)  # 6h default
        for ex in self.exchanges.values():
            try:
                fiat = ex.get_fiat_balance(cached=True)  # use cache — avoids extra API call every hour
                ex_name = ex.name if hasattr(ex, 'name') else str(ex)
                last_warned = self._low_balance_warned.get(ex_name, 0)
                if fiat < threshold and (not last_warned or time.time() - last_warned > cooldown):
                    self._low_balance_warned[ex_name] = time.time()
                    if self.notifier:
                        currency = ex.get_fiat_currency() if hasattr(ex, 'get_fiat_currency') else 'EUR'
                        self.notifier.notify_low_balance(fiat, currency)
                        log.warning(f"Low balance {ex_name}: {fiat:.2f} {currency}")
                elif fiat >= threshold:
                    self._low_balance_warned.pop(ex_name, None)  # reset when balance recovers
            except Exception:
                pass
    def _auto_buy_escrow_check(self):
        """Auto-trigger buy_escrow_norev for each exchange with sufficient fiat balance.
        Runs every check_interval_sec (default 600s). Multiple offers can be active simultaneously.
        """
        cfg = self.config.get("auto_buy_escrow", {})
        if not cfg.get("enabled"):
            return
        now = time.time()
        interval = cfg.get("check_interval_sec", 600)
        if now - self._last_auto_buy_check < interval:
            return
        self._last_auto_buy_check = now
        if self.paused or not self.notifier:
            return
        # Don't trigger new buys while an escrow is waiting to be funded — prevents
        # concurrent withdrawals competing for the same UTXOs (would leave one underfunded).
        with self._escrow_lock:
            unfunded = [oid for oid, info in self.pending_escrows.items() if not info.get("funded")]
        if unfunded:
            log.info(f"auto_buy_escrow: skipping — {len(unfunded)} offer(s) still awaiting funding")
            return
        amounts = cfg.get("amounts", [500, 400, 300, 200])
        mode = cfg.get("mode", "norev")
        exclude_methods = [] if mode == "withrev" else cfg.get("exclude_methods", ["revolut"])
        for ex in self.exchanges.values():
            try:
                fiat_balance = ex.get_fiat_balance()
                currency = ex.get_fiat_currency() if hasattr(ex, "get_fiat_currency") else "?"
                for amount in amounts:
                    if fiat_balance >= amount * 0.98:
                        log.info(f"auto_buy_escrow: triggering {ex.name} {amount:.0f} {currency} (balance: {fiat_balance:.2f})")
                        self.notifier.trigger_auto_buy_escrow(ex, float(amount), exclude_methods)
                        break  # one offer per exchange per cycle
            except Exception as e:
                log.warning(f"auto_buy_escrow check {ex.name}: {e}")

    def _auto_fund_wallet_check(self):
        """Create a Peach offer for any unallocated confirmed BTC on the hot wallet.
        Runs every 5 min. Only triggers when unallocated balance >= min_wallet_fund_sats
        (config key auto_buy_escrow.min_wallet_fund_sats, default 120000 ≈ 100 CHF).
        """
        cfg = self.config.get("auto_buy_escrow", {})
        if not cfg.get("enabled") or self.paused or not self.notifier:
            return
        now = time.time()
        if now - self._last_auto_fund_wallet_check < 300:
            return
        self._last_auto_fund_wallet_check = now

        pconfig = self.config.get("platforms", {}).get("peach", {})
        hot_wallet_addr = pconfig.get("refund_address", "")
        if not hot_wallet_addr:
            return
        try:
            from fund_from_wallet import get_utxos
            utxos = get_utxos(hot_wallet_addr)
            confirmed_sats = sum(u["value"] for u in utxos
                                 if u.get("status", {}).get("confirmed", False))
        except Exception as e:
            log.debug(f"auto_fund_wallet: UTXO check failed: {e}")
            return

        with self._escrow_lock:
            # Don't create a new offer while any escrow is still waiting to be funded —
            # those UTXOs are already claimed.
            pending_unfunded = [info for info in self.pending_escrows.values()
                                if not info.get("funded")]
            reserved_sats = sum(info["amount_sats"] for info in pending_unfunded)

        if pending_unfunded:
            return

        # Minimum CHF value to justify a new offer — converted to sats at current price
        min_chf = cfg.get("min_wallet_fund_chf", 100)
        fee_buffer = 5000
        unallocated_sats = confirmed_sats - reserved_sats
        try:
            spot_chf = SpotPriceProvider.get_spot_chf()
            min_threshold = int(min_chf / spot_chf * 1e8)
        except Exception:
            min_threshold = 120_000  # fallback ~100 CHF if price unavailable
        if unallocated_sats < min_threshold + fee_buffer:
            log.debug(f"auto_fund_wallet: {unallocated_sats:,} sats < {min_chf} CHF ({min_threshold:,} sats) — skipping")
            return

        exclude_methods = cfg.get("exclude_methods", ["revolut"])
        log.info(f"auto_fund_wallet: {unallocated_sats:,} unallocated sats on hot wallet — creating offer")
        self.notifier.trigger_auto_fund_wallet(unallocated_sats - fee_buffer, exclude_methods)

    def _auto_consolidate_utxos(self):
        """Consolidate hot wallet UTXOs when idle and fees are low. Runs every 30 min."""
        now = datetime.now()
        if self._last_consolidation_check and (now - self._last_consolidation_check).total_seconds() < 1800:
            return
        self._last_consolidation_check = now
        # Only consolidate when no escrow funding is actively in progress (avoid UTXO conflicts).
        # Funded offers waiting for a buyer match don't use UTXOs — safe to consolidate.
        with self._escrow_lock:
            funding_in_progress = any(v.get("funding_in_progress") for v in self.pending_escrows.values())
        if funding_in_progress:
            return
        try:
            from fund_from_wallet import consolidate_utxos
            result = consolidate_utxos(self.config)
            if result:
                log.info(f"UTXO consolidation: merged {result['utxos_merged']} UTXOs, fee {result['fee']} sats, txid {result['txid']}")
                if self.notifier:
                    self.notifier._send(
                        f"<b>UTXO Konsolidierung</b>\n"
                        f"{result['utxos_merged']} UTXOs → 1\n"
                        f"Total: {result['total_sats']:,} sats\n"
                        f"Fee: {result['fee']:,} sats\n"
                        f"TXID: <code>{result['txid'][:16]}...</code>"
                    )
        except Exception as e:
            log.warning(f"UTXO consolidation: {e}")

    def _load_pending_refunds(self):
        """Load pending refunds from file (persists across restarts). Thread-safe."""
        import os
        refund_file = os.path.join(os.path.dirname(__file__), "..", "pending_refunds.json")
        with self._escrow_lock:
            if os.path.exists(refund_file):
                try:
                    with open(refund_file) as f:
                        self._pending_refunds = json.load(f)
                    if self._pending_refunds:
                        total = sum(v["amount_sats"] for v in self._pending_refunds.values())
                        log.info(f"Loaded {len(self._pending_refunds)} pending refunds ({total:,} sats)")
                except Exception as e:
                    log.warning(f"Failed to load pending refunds: {e}")
    def register_refund(self, offer_id, platform_name="peach"):
        """Register a cancelled offer for refund monitoring. Fetches escrow info from Peach."""
        if str(offer_id) in self._pending_refunds:
            return  # already tracked
        platform = self.platforms.get(platform_name)
        if not platform:
            return
        try:
            platform._ensure_auth()
            r = platform.session.get(f"{platform.base_url}/offer/{offer_id}/escrow", timeout=15)
            if r.status_code != 200:
                log.debug(f"No escrow info for {offer_id}: HTTP {r.status_code}")
                return
            d = r.json()
            escrow_addr = d.get("escrow", "")
            amounts = d.get("funding", {}).get("amounts", [])
            amount = amounts[0] if amounts else 0
            if not escrow_addr or amount <= 0:
                log.debug(f"Offer {offer_id}: no funded escrow")
                return
            # Check if still has balance (skip if already refunded)
            md = _mempool_get(f"https://mempool.space/api/address/{escrow_addr}")
            balance = (md.get("chain_stats", {}).get("funded_txo_sum", 0)
                      - md.get("chain_stats", {}).get("spent_txo_sum", 0)
                      + md.get("mempool_stats", {}).get("funded_txo_sum", 0)
                      - md.get("mempool_stats", {}).get("spent_txo_sum", 0))
            if balance <= 0:
                log.info(f"Offer {offer_id}: escrow already empty, no refund tracking needed")
                return
            self._pending_refunds[str(offer_id)] = {
                "escrow_addr": escrow_addr,
                "amount_sats": amount,
                "created": datetime.now().isoformat()
            }
            self._save_pending_refunds()
            log.info(f"Tracking refund for offer {offer_id}: {amount:,} sats at {escrow_addr[:30]}...")
            if self.notifier:
                self.notifier._send(
                    f"<b>Refund-Monitoring</b>\n"
                    f"Offer {offer_id}: {amount:,} sats\n"
                    f"Warte auf Rückzahlung..."
                )
        except Exception as e:
            log.warning(f"Failed to register refund for {offer_id}: {e}")
    def _save_pending_refunds(self):
        """Save pending refunds to file (thread-safe via _escrow_lock)."""
        import os
        refund_file = os.path.join(os.path.dirname(__file__), "..", "pending_refunds.json")
        with self._escrow_lock:
            try:
                with open(refund_file, "w") as f:
                    json.dump(self._pending_refunds, f, indent=2)
            except Exception as e:
                log.debug(f"Failed to save pending refunds: {e}")
    def _check_pending_refunds(self):
        if not self._pending_refunds:
            self._auto_broadcast_refunds(cached_offers=self._cached_active_offers)
            return
        now = datetime.now()
        # Check every 5 minutes
        if self._last_refund_check and (now - self._last_refund_check).total_seconds() < 300:
            return
        self._last_refund_check = now
        completed = []
        for oid, info in list(self._pending_refunds.items()):
            try:
                addr = info["escrow_addr"]
                d = _mempool_get(f"https://mempool.space/api/address/{addr}")
                funded = d.get("chain_stats", {}).get("funded_txo_sum", 0)
                spent = d.get("chain_stats", {}).get("spent_txo_sum", 0)
                balance = funded - spent
                mfunded = d.get("mempool_stats", {}).get("funded_txo_sum", 0)
                mspent = d.get("mempool_stats", {}).get("spent_txo_sum", 0)
                balance += mfunded - mspent
                if balance <= 0:
                    log.info(f"Refund received for offer {oid} ({info['amount_sats']} sats)")
                    completed.append(oid)
                    if self.notifier:
                        self.notifier._send(
                            f"<b>Refund erhalten!</b>\n"
                            f"Offer {oid}: {info['amount_sats']:,} sats zurück"
                        )
            except Exception as e:
                log.debug(f"Refund check {oid}: {e}")
        for oid in completed:
            del self._pending_refunds[oid]
        if completed:
            self._save_pending_refunds()
        if self._pending_refunds:
            remaining = sum(v["amount_sats"] for v in self._pending_refunds.values())
            log.debug(f"Pending refunds: {len(self._pending_refunds)} offers, {remaining:,} sats")
        if completed and not self._pending_refunds:
            log.info("All pending refunds received!")
            if self.notifier:
                self.notifier._send("<b>Alle Refunds erhalten!</b>")
        # Also check for unbroadcast refund PSBTs
        self._auto_broadcast_refunds()

    def _auto_broadcast_refunds(self, cached_offers=None):
        """Sign and broadcast any unbroadcast refund PSBTs from Peach."""
        now = datetime.now()
        # Check every 10 minutes
        if not hasattr(self, '_last_refund_broadcast_check'):
            self._last_refund_broadcast_check = None
        if self._last_refund_broadcast_check and (now - self._last_refund_broadcast_check).total_seconds() < 600:
            return
        self._last_refund_broadcast_check = now

        for pname, platform in self.platforms.items():
            try:
                platform._ensure_auth()
                offers = cached_offers.get(pname) if cached_offers else None
                if offers is None:
                    offers = platform.get_active_offers()
                for o in offers:
                    raw = o.raw_data
                    refund_psbt = raw.get("refundTx", "")
                    refunded = raw.get("refunded", False)
                    if not refund_psbt or refunded:
                        continue
                    oid = raw.get("id", "")
                    amount = raw.get("amount", 0)
                    try:
                        from coincurve import PrivateKey
                        from release_escrow import build_finalized_tx
                        import requests as _req
                        # Get derivation path from escrow status
                        esc = platform.get_escrow_status(oid)
                        deriv = esc.get("funding", {}).get("derivationPath", "")
                        deriv_id = int(deriv.split("/")[-1]) if deriv else int(oid)
                        key_hex = platform._get_escrow_privkey_hex(deriv_id)
                        privkey = PrivateKey(bytes.fromhex(key_hex))
                        tx_hex = build_finalized_tx(refund_psbt, privkey)
                        if not tx_hex:
                            continue
                        r = _req.post("https://mempool.space/api/tx", data=tx_hex, timeout=15)
                        if r.status_code == 200:
                            log.info(f"{pname}: auto-refund broadcast for offer {oid} ({amount:,} sats)")
                            if self.notifier:
                                self.notifier._send(
                                    f"<b>Refund gebroadcastet!</b>\n"
                                    f"Offer {oid}: {amount:,} sats\n"
                                    f"TX: <code>{r.text[:16]}...</code>"
                                )
                        else:
                            # Already spent or other error — not critical
                            log.debug(f"{pname}: refund broadcast {oid}: {r.text[:100]}")
                    except Exception as e:
                        log.debug(f"{pname}: auto-refund {oid}: {e}")
            except Exception as e:
                log.debug(f"{pname}: auto-refund check: {e}")
    def _timed(self, label, fn, *args):
        """Run fn and log duration if >1s."""
        t0 = time.time()
        try:
            return fn(*args)
        finally:
            dt = time.time() - t0
            if dt > 1.0:
                log.info(f"⏱ {label}: {dt:.1f}s")
            else:
                log.debug(f"⏱ {label}: {dt:.2f}s")

    def _process_platform(self, name, platform):
        # Quick health check: if auth fails, skip entire platform this tick
        try:
            platform._ensure_auth()
            # Platform reachable — clear outage state if it was down
            if name in self._platform_down_since:
                down_sec = int(time.time() - self._platform_down_since.pop(name))
                self._platform_down_notified.discard(name)
                log.info(f"{name}: API wieder erreichbar (war {down_sec}s down)")
                if self.notifier:
                    self.notifier._send(f"✅ <b>{name} API wieder erreichbar</b>\n(war {down_sec}s nicht verfügbar)")
        except Exception as e:
            log.warning(f"{name}: auth failed, skipping tick: {e}")
            now = time.time()
            if name not in self._platform_down_since:
                self._platform_down_since[name] = now
            down_sec = now - self._platform_down_since[name]
            if down_sec > 300 and name not in self._platform_down_notified:
                self._platform_down_notified.add(name)
                log.error(f"{name}: API seit {down_sec:.0f}s nicht erreichbar!")
                if self.notifier:
                    self.notifier._send(
                        f"🚨 <b>{name} API nicht erreichbar</b>\n"
                        f"Seit {int(down_sec // 60)} Minuten ausgefallen.\n"
                        f"Fehler: {str(e)[:200]}")
            return
        # Sequential: create offers + fund first (must complete before match checking)
        self._timed(f"{name}/offers", self._create_offers, name, platform)
        if self.auto_fund: self._timed(f"{name}/fund", self._fund_escrows, name, platform)
        # Parallel: contracts, matches, stale checks are independent
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="tick") as pool:
            futures = {
                pool.submit(self._timed, f"{name}/contracts", self._check_contracts, name, platform): "contracts",
                pool.submit(self._timed, f"{name}/matches", self._check_matches, name, platform): "matches",
                pool.submit(self._timed, f"{name}/stale", self._check_stale_offers, name, platform): "stale",
            }
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    log.error(f"{name}/{futures[f]}: {e}")
    def _create_offers(self, name, platform):
        pconfig = self.config.get("platforms",{}).get(name,{})
        min_sats = pconfig.get("min_amount_sats",50000)
        max_sats = pconfig.get("max_amount_sats",500000)
        payment_methods = pconfig.get("payment_methods",{"EUR":["sepa","instantSepa","revolut","wise"],"CHF":["revolut","wise","twint"],"USD":["revolut","wise"]})
        premium = self.pricer.get_premium(name, platform)
        try: active = platform.get_active_offers()
        except Exception as e: log.warning(f"{name}: {e}"); return
        # Cache for _auto_broadcast_refunds to avoid redundant API call
        self._cached_active_offers[name] = active
        # Sync existing funded offers into pending_escrows (e.g. after restart)
        # Only sync offers with FUNDED escrow status; mark as funded=True to skip auto-funding
        for offer in active:
            with self._escrow_lock:
                if offer.id not in self.pending_escrows:
                    try:
                        escrow_info = platform.get_escrow_status(offer.id)
                        f = escrow_info.get('funding', {})
                        fstatus = f.get('status', '') if isinstance(f, dict) else str(f)
                        if fstatus not in ('FUNDED', 'MEMPOOL'):
                            continue  # Skip canceled, wrong-funded, or unfunded offers
                        escrow_addr = escrow_info.get('escrow', escrow_info.get('escrows', {}).get('bitcoin', ''))
                        amount = offer.max_sats or max_sats
                        # Use saved state (from premium reduction timer) if available, else real API date
                        saved = self._escrow_state.get(str(offer.id), {})
                        if saved.get("funded_at"):
                            funded_at = saved["funded_at"]
                        else:
                            real_date = offer.raw_data.get("publishingDate") or offer.raw_data.get("creationDate") or datetime.now().isoformat()
                            funded_at = real_date.replace("Z", "+00:00") if real_date.endswith("Z") else real_date
                        # Re-add funded offers even if previously in _contracted_offers
                        if offer.id in self._contracted_offers:
                            self._contracted_offers.discard(offer.id)
                            self._save_contracted_offers()
                            log.info(f"{name}: re-activated contracted offer {offer.id} (still FUNDED)")
                        self.pending_escrows[offer.id] = {"platform": name, "escrow_address": escrow_addr, "amount_sats": amount, "funded": True, "funded_at": funded_at, "premium": saved.get("premium", offer.premium_pct), "sepa_account_index": saved.get("sepa_account_index", 0)}
                        log.info(f"{name}: synced funded offer {offer.id} into pending_escrows")
                    except Exception as e:
                        log.debug(f"{name}: escrow status {offer.id[:12]}: {e}")
        # Offers are created manually via /buy_escrow — no auto-create limit
    def _fund_escrows(self, name, platform):
        with self._escrow_lock:
            to_fund = {oid:info for oid,info in self.pending_escrows.items() if info["platform"]==name and not info["funded"] and not info.get("funding_in_progress")}
        if not to_fund: return
        exchange = self.get_best_exchange()
        if not exchange: return
        for oid, info in to_fund.items():
            try:
                addr = info.get("escrow_address","")
                if not addr:
                    addr = platform.get_escrow_address(oid)
                    info["escrow_address"] = addr
                if not addr: continue
                spot = exchange.get_spot_price()
                fiat = (info["amount_sats"]/1e8) * spot * 1.005
                result = exchange.buy_and_withdraw(fiat, addr)
                info["funded"] = True
                info["funded_at"] = datetime.now().isoformat()
                log.info(f"{name}: funded {oid[:12]} ({result['total_fiat']:.2f} {result.get('currency','EUR')})")
                if self.notifier: self.notifier.notify_escrow_funded(oid, result["total_btc"], result["total_fiat"], result.get("currency", "CHF"))
            except Exception as e:
                log.error(f"{name}: fund {oid[:12]}: {e}")
    def _check_matches(self, name, platform):
        # Payment data mapping for accepting trade requests
        pconfig = self.config.get("platforms",{}).get(name,{})
        raw = pconfig.get("payment_data_raw", {})
        info = pconfig.get("payment_data_info", {})
        revolut_user = raw.get("revolut", "")
        wise_user = raw.get("wise", "")
        n26_iban = raw.get("instantSepa", "")
        sepa_iban = raw.get("sepa", "")
        # Raw data for hashing (must match what was used when creating the offer)
        payment_data_map = {
            "twint": raw.get("twint", ""),
            "revolut": json.dumps({"userName": revolut_user, "reference": ""}),
            "sepa": sepa_iban,
            "instantSepa": n26_iban,
            "wise": json.dumps({"userName": wise_user, "reference": ""}),
            "solanausdt": raw.get("solanausdt", ""),
            "arbitrumusdt": raw.get("arbitrumusdt", ""),
            "ethereumusdt": raw.get("ethereumusdt", ""),
        }
        # Structured payment info for encryption (what the buyer sees)
        beneficiary = info.get("beneficiary", "")
        payment_info_map = {
            "twint": info.get("twint", {"phone": raw.get("twint", "")}),
            "revolut": info.get("revolut", {"userName": revolut_user, "reference": ""}),
            "sepa": info.get("sepa", {"iban": sepa_iban, "beneficiary": beneficiary}),
            "instantSepa": info.get("instantSepa", {"iban": n26_iban, "beneficiary": beneficiary}),
            "wise": info.get("wise", {"userName": wise_user, "reference": ""}),
            "solanausdt": info.get("solanausdt", {"address": raw.get("solanausdt", "")}),
            "arbitrumusdt": info.get("arbitrumusdt", {"address": raw.get("arbitrumusdt", "")}),
            "ethereumusdt": info.get("ethereumusdt", {"address": raw.get("ethereumusdt", "")}),
        }

        sepa_accounts = pconfig.get("sepa_accounts", [])

        with self._escrow_lock:
            funded_offers = {oid:info for oid,info in self.pending_escrows.items() if info["platform"]==name and info.get("funded")}
        for oid, info in funded_offers.items():
            # Skip if in exponential backoff from previous 401
            skip_until = info.get("_401_skip_until", 0)
            if skip_until and time.time() < skip_until:
                continue

            # Per-offer SEPA account rotation: use the account index stored at offer creation
            offer_payment_data_map = dict(payment_data_map)
            offer_payment_info_map = dict(payment_info_map)
            sepa_idx = info.get("sepa_account_index", 0)
            if sepa_accounts and sepa_idx < len(sepa_accounts):
                acct = sepa_accounts[sepa_idx]
                iban = acct["iban"].replace(" ", "")
                bic = acct.get("bic", "")
                ben = acct.get("beneficiary", beneficiary)
                offer_payment_data_map["sepa"] = iban
                offer_payment_data_map["instantSepa"] = iban
                offer_payment_info_map["sepa"] = {"iban": iban, "beneficiary": ben, "bic": bic}
                offer_payment_info_map["instantSepa"] = {"iban": iban, "beneficiary": ben, "bic": bic}

            try:
                # Use v069 API (the working endpoint!)
                if hasattr(platform, 'check_trade_requests'):
                    trade_requests = platform.check_trade_requests(oid)
                    # Successful API call — reset any 401 counter
                    if info.get("_401_count"):
                        with self._escrow_lock:
                            if oid in self.pending_escrows:
                                self.pending_escrows[oid].pop("_401_count", None)
                    if not trade_requests:
                        continue
                    # Try all trade requests until one with a supported method is found
                    accepted = False
                    for tr in trade_requests:
                        buyer_id = tr.get("userId", tr.get("user", {}).get("id", ""))
                        method = tr.get("paymentMethod", "")
                        currency = tr.get("currency", "")

                        # Get the raw payment data for this method
                        raw_data = offer_payment_data_map.get(method, "")
                        if not raw_data:
                            log.warning(f"{name}: No payment data for method {method}, trying next request")
                            continue

                        method_info = offer_payment_info_map.get(method, {})
                        platform.accept_trade_request(oid, buyer_id, method, raw_data, trade_request_data=tr, payment_info=method_info)
                        log.info(f"{name}: ACCEPTED trade request from {buyer_id[:12]} via {method}")
                        if self.notifier:
                            self.notifier.notify_match(oid, buyer_id)
                        self.trade_logger.log_event(name, "match_accepted", f"{oid}:{buyer_id}")
                        accepted = True
                        break
                    if not accepted:
                        log.warning(f"{name}: No supported payment method in {len(trade_requests)} trade request(s) for {oid}")
                else:
                    # Fallback to v1 (probably won't work)
                    matches = platform.check_matches(oid)
                    if not matches: continue
                    m = matches[0]
                    platform.accept_match(oid, m.id)
                    log.info(f"{name}: MATCH {m.id[:12]}")
                    if self.notifier:
                        self.notifier.notify_match(oid, m.id)
                    self.trade_logger.log_event(name, "match", f"{oid}:{m.id}")
            except Exception as e:
                err_str = str(e)
                log.warning(f"{name}: matches {oid[:12]}: {err_str}")
                if "401" in err_str and info.get("funded"):
                    # Check response body for permanent vs transient 401
                    err_lower = err_str.lower()
                    is_permanent = any(kw in err_lower for kw in ("not found", "expired", "deleted", "canceled", "cancelled"))

                    # Grace period: freshly funded offers get 401 until escrow confirms on-chain.
                    # Don't count these as failures — just wait 5 minutes and retry.
                    funded_at_str = info.get("funded_at", "")
                    try:
                        from datetime import timezone
                        funded_dt = datetime.fromisoformat(funded_at_str.replace("Z", "+00:00"))
                        age_minutes = (datetime.now(timezone.utc) - funded_dt).total_seconds() / 60
                    except Exception:
                        age_minutes = 999
                    if not is_permanent and age_minutes < 20:
                        with self._escrow_lock:
                            if oid in self.pending_escrows:
                                self.pending_escrows[oid]["_401_skip_until"] = time.time() + 300
                        log.info(f"{name}: {oid[:12]} 401 — escrow < 20min old ({age_minutes:.0f}min), retry in 5min")
                        continue

                    fails = info.get("_401_count", 0) + 1
                    max_fails = 2 if is_permanent else 5
                    with self._escrow_lock:
                        if oid in self.pending_escrows:
                            self.pending_escrows[oid]["_401_count"] = fails
                            # Exponential backoff: skip next N ticks
                            self.pending_escrows[oid]["_401_skip_until"] = time.time() + min(30 * (2 ** (fails - 1)), 600)
                    if fails >= max_fails:
                        self._contracted_offers.add(oid)
                        self._save_contracted_offers()
                        with self._escrow_lock:
                            self.pending_escrows.pop(oid, None)
                        reason = "permanent" if is_permanent else f"transient x{fails}"
                        log.info(f"{name}: removed {oid[:12]} from polling (401 {reason})")
                    else:
                        log.info(f"{name}: {oid[:12]} 401 ({fails}/{max_fails}) — backoff {30 * (2 ** (fails - 1))}s")
    def _check_contracts(self, name, platform):
        # Prune _contracted_offers to prevent unbounded growth (keep newest 50 by lexicographic sort)
        if len(self._contracted_offers) > 100:
            self._contracted_offers = set(sorted(self._contracted_offers)[-50:])
        try: contracts = platform.get_contracts()
        except Exception as e: log.warning(f"{name}: contracts: {e}"); return
        for c in contracts:
            # When a contract exists for an offer, remove offer from pending_escrows to stop trade request polling
            # But preserve buy_data for profit calculation
            offer_id = (c.id.split('-')[0] if '-' in c.id else getattr(c, 'offer_id', None)) or ''
            if offer_id:
                self._contracted_offers.add(offer_id)
                with self._escrow_lock:
                    removed = self.pending_escrows.pop(offer_id, None)
                    if removed and removed.get("buy_data"):
                        self._buy_data_cache[offer_id] = removed["buy_data"]
                        # Prevent unbounded cache growth
                        if len(self._buy_data_cache) > 50:
                            oldest = list(self._buy_data_cache.keys())[0]
                            self._buy_data_cache.pop(oldest, None)
            if c.status == OfferStatus.PAYMENT_RECEIVED:
                if c.id not in self._notified_contracts:
                    log.info(f"{name}: PAYMENT RECEIVED {c.id[:16]}")
                    self._notified_contracts.add(c.id)
                if self.auto_confirm:
                    try:
                        # Try to get PSBT and sign for release
                        if hasattr(platform, 'sign_release_psbt'):
                            psbt_data = platform.sign_release_psbt(c.id)
                            if psbt_data:
                                log.info(f"{name}: PSBT ready for {c.id[:16]} - manual signing needed")
                                if self.notifier:
                                    self.notifier._send(f"<b>PSBT Ready!</b>\nContract <code>{c.id[:16]}</code>\nSign and release manually or auto-release will attempt.")
                        # Simple confirm (without release TX)
                        platform.confirm_payment(c.id)
                        if c.id not in self._recorded_contracts:
                            self._recorded_contracts.add(c.id)
                            self._record_trade(name, c)
                    except Exception as e:
                        log.error(f"{name}: confirm: {e}")
            elif c.status == OfferStatus.DISPUTE:
                log.warning(f"{name}: DISPUTE {c.id}")
                if self.notifier and c.id not in self._notified_contracts:
                    self.notifier.notify_dispute(c.id)
                    self._notified_contracts.add(c.id)
            elif c.status == OfferStatus.COMPLETED:
                with self._escrow_lock:
                    for oid in (c.offer_id, c.id):
                        if oid and oid in self.pending_escrows:
                            removed = self.pending_escrows.pop(oid)
                            if removed.get("buy_data"):
                                self._buy_data_cache[oid] = removed["buy_data"]
                            break
                if c.id not in self._recorded_contracts:
                    self._recorded_contracts.add(c.id)
                    self._record_trade(name, c)
        if self._contracted_offers != self._contracted_offers_snapshot:
            self._save_contracted_offers()
            self._contracted_offers_snapshot = set(self._contracted_offers)

    def _load_contracted_offers(self):
        try:
            if os.path.exists(self._contracted_offers_file):
                with open(self._contracted_offers_file) as f:
                    self._contracted_offers = set(json.load(f))
                    log.info(f"Loaded {len(self._contracted_offers)} contracted offers from disk")
        except Exception as e:
            log.debug(f"Failed to load contracted offers: {e}")

    def _save_contracted_offers(self):
        """Write contracted offers to disk — debounced to max once per 60s."""
        now = time.time()
        if now - self._last_contracted_offers_save < 60:
            return
        self._last_contracted_offers_save = now
        try:
            ids = sorted(self._contracted_offers)
            if len(ids) > 100:
                ids = ids[-100:]
                self._contracted_offers = set(ids)
            with open(self._contracted_offers_file, "w") as f:
                json.dump(ids, f)
        except Exception as e:
            log.debug(f"Failed to save contracted offers: {e}")

    def _load_escrow_state(self):
        """Load persisted escrow state (funded_at timers after premium reductions)."""
        try:
            if os.path.exists(self._escrow_state_file):
                with open(self._escrow_state_file) as f:
                    return json.load(f)
        except Exception as e:
            log.debug(f"Failed to load escrow state: {e}")
        return {}

    def _save_escrow_state(self):
        """Save escrow state so premium reduction timers survive restarts."""
        try:
            with open(self._escrow_state_file, "w") as f:
                json.dump(self._escrow_state, f, indent=2)
        except Exception as e:
            log.debug(f"Failed to save escrow state: {e}")

    @staticmethod
    def _parse_funded_at(funded_at_str):
        """Parse funded_at string to naive local datetime (handles both UTC ISO and local)."""
        dt = datetime.fromisoformat(funded_at_str)
        if dt.tzinfo is not None:
            # Convert UTC to local naive datetime
            import time as _time
            utc_ts = dt.timestamp()
            dt = datetime.fromtimestamp(utc_ts)
        return dt

    def _check_stale_offers(self, name, platform):
        """Reduce premium on funded offers without match after X hours via PATCH.
        Also cleans up very old pending_escrows entries (>7 days)."""
        now = datetime.now()
        # Check every 10 minutes
        if self._last_stale_check and (now - self._last_stale_check).total_seconds() < 600:
            return
        self._last_stale_check = now

        # Clean up stale pending_escrows (older than 7 days)
        MAX_ESCROW_AGE_DAYS = 7
        with self._escrow_lock:
            stale_ids = []
            for oid, info in self.pending_escrows.items():
                funded_at = info.get("funded_at")
                if funded_at:
                    try:
                        age_days = (now - self._parse_funded_at(funded_at)).total_seconds() / 86400
                        if age_days > MAX_ESCROW_AGE_DAYS:
                            stale_ids.append(oid)
                    except Exception:
                        pass
            for oid in stale_ids:
                self.pending_escrows.pop(oid, None)
                self._escrow_state.pop(str(oid), None)
                log.info(f"{name}: removed stale pending_escrow {oid[:12]} (>7 days old)")
            if stale_ids:
                self._save_escrow_state()
        pconfig = self.config.get("platforms",{}).get(name,{})
        reduction_hours = pconfig.get("premium_reduction_hours", 24)
        reduction_step = pconfig.get("premium_reduction_step", 0.5)
        floor = pconfig.get("dynamic_pricing", {}).get("floor_pct", pconfig.get("premium_floor", 4.0))
        with self._escrow_lock:
            funded = {oid:info for oid,info in self.pending_escrows.items()
                     if info["platform"]==name and info.get("funded") and info.get("funded_at")}
        for oid, info in funded.items():
            try:
                funded_at = self._parse_funded_at(info["funded_at"])
                hours_waiting = (now - funded_at).total_seconds() / 3600
                if hours_waiting < reduction_hours:
                    continue
                current_premium = info.get("premium", self.pricer.current_premiums.get(name, 6.0))
                new_premium = max(round(current_premium - reduction_step, 1), floor)
                if new_premium >= current_premium:
                    log.debug(f"{name}: offer {oid[:12]} at floor {floor}%, no further reduction")
                    continue
                # Directly patch premium on the live offer (no cancel needed)
                if hasattr(platform, 'update_premium') and platform.update_premium(oid, new_premium):
                    log.info(f"{name}: offer {oid[:12]} premium {current_premium}% -> {new_premium}% (waited {hours_waiting:.0f}h)")
                    info["premium"] = new_premium
                    info["funded_at"] = now.isoformat()  # reset timer for next reduction
                    # Persist so timer survives restarts
                    self._escrow_state[str(oid)] = {"funded_at": now.isoformat(), "premium": new_premium, "sepa_account_index": info.get("sepa_account_index", 0)}
                    self._save_escrow_state()
                    if self.notifier:
                        self.notifier._send(
                            f"<b>Premium gesenkt</b>\n"
                            f"Offer {oid}: {current_premium}% \u2192 {new_premium}%\n"
                            f"Kein Match seit {hours_waiting:.0f}h"
                        )
                else:
                    log.debug(f"{name}: offer {oid[:12]} premium patch failed, may not be online")
            except Exception as e:
                log.warning(f"{name}: stale check {oid[:12]}: {e}")
    def _record_trade(self, pname, contract):
        currency = getattr(contract, 'currency', 'EUR')
        try: spot_now = SpotPriceProvider.get_spot(currency)
        except Exception as e: log.warning(f"Spot price for trade record: {e}"); spot_now = 0
        btc = contract.amount_sats / 1e8
        sell_price = contract.price_fiat

        # Look up actual buy data from pending_escrows or cache
        buy_data = {}
        offer_id = getattr(contract, 'offer_id', '') or (contract.id.split('-')[0] if '-' in contract.id else '')
        for oid in (offer_id, contract.id):
            if oid:
                # Check pending_escrows first, then cache
                info = self.pending_escrows.get(oid, {})
                buy_data = info.get("buy_data", {}) or self._buy_data_cache.pop(oid, {})
                if buy_data:
                    break

        # Kraken withdrawal fee: fixed 0.000015 BTC
        KRAKEN_WITHDRAWAL_FEE_BTC = 0.000015

        if buy_data.get("fiat_spent"):
            # Use actual buy price from Kraken
            buy_price = buy_data["fiat_spent"]
            efee = buy_data.get("exchange_fee", 0)
            spot_at_buy = buy_data.get("spot_at_buy", 0)
            buy_currency = buy_data.get("buy_currency", currency)
            funding_fee_sats = buy_data.get("funding_fee_sats", 0)
            funding_fee = funding_fee_sats / 1e8 * spot_now if spot_now else 0
        else:
            # Fallback: estimate from current spot (old trades without buy_data)
            # Determine actual exchange buy currency (may differ from sell currency)
            ex = list(self.exchanges.values())[0] if self.exchanges else None
            buy_currency = ex.get_fiat_currency() if ex and hasattr(ex, 'get_fiat_currency') else currency
            try:
                spot_buy = SpotPriceProvider.get_spot(buy_currency) if buy_currency != currency else spot_now
            except Exception:
                spot_buy = spot_now
            buy_price = btc * spot_buy
            efee = buy_price * 0.0026
            spot_at_buy = spot_buy
            # Estimate funding fee: ~150 vbytes × 5 sat/vB = 750 sats
            funding_fee = 750 / 1e8 * spot_buy if spot_buy else 0

        # Peach 2% fee is paid by the buyer, not the seller — excluded from our cost
        pfee = 0
        # All fees in CHF (base accounting currency)
        spot_chf = None
        try: spot_chf = SpotPriceProvider.get_spot("CHF")
        except Exception: pass
        spot_for_fees = spot_at_buy if spot_at_buy else (spot_chf or spot_now)
        withdrawal_fee = KRAKEN_WITHDRAWAL_FEE_BTC * spot_for_fees if spot_for_fees else 0

        # Convert sell_price to CHF for unified accounting
        if currency == "CHF":
            sell_price_chf = sell_price
        else:
            # Convert via BTC: sell_price / spot_sell * spot_chf
            if spot_now and spot_chf:
                sell_price_chf = sell_price / spot_now * spot_chf
            else:
                sell_price_chf = sell_price  # fallback: no conversion possible

        # Convert buy_price to CHF if needed
        if buy_currency == "CHF":
            buy_price_chf = buy_price
            efee_chf = efee
        else:
            if spot_now and spot_chf:
                rate = spot_chf / SpotPriceProvider.get_spot(buy_currency) if buy_currency != "CHF" else 1
                buy_price_chf = buy_price * rate
                efee_chf = efee * rate
            else:
                buy_price_chf = buy_price
                efee_chf = efee

        # Net profit in CHF
        net = sell_price_chf - buy_price_chf - efee_chf - withdrawal_fee - funding_fee

        self.trade_logger.log_trade(
            platform=pname,
            exchange=list(self.exchanges.keys())[0] if self.exchanges else "",
            contract_id=contract.id, amount_sats=contract.amount_sats,
            buy_price=buy_price, sell_price=sell_price,
            currency=currency, buy_currency=buy_currency,
            sell_price_chf=round(sell_price_chf, 2),
            premium_pct=((sell_price / (btc * spot_now) - 1) * 100) if (btc * spot_now) > 0 else 0,
            exchange_fee=efee, platform_fee=pfee,
            network_fee=withdrawal_fee + funding_fee,
            net_profit=net, payment_method=contract.payment_method,
            withdrawal_fee=withdrawal_fee, funding_fee=funding_fee,
            spot_at_buy=spot_at_buy, spot_at_sell=spot_now)
        self.daily_volume_sats += contract.amount_sats
        log.info(f"{pname}: COMPLETE {contract.id[:12]} profit={net:.2f} {currency} (buy={buy_price:.2f} sell={sell_price:.2f} fees={efee+pfee+withdrawal_fee+funding_fee:.2f})")
    def get_status(self):
        uptime = datetime.now() - self._start_time
        h, m = divmod(int(uptime.total_seconds()) // 60, 60)
        d, h = divmod(h, 24)
        uptime_str = f"{d}d {h}h {m}m" if d else f"{h}h {m}m"
        return {"paused":self.paused,"uptime":uptime_str,"daily_volume_sats":self.daily_volume_sats,"pending_escrows":len(self.pending_escrows),"funded_escrows":sum(1 for e in self.pending_escrows.values() if e.get("funded")),"platforms":{n:p.get_status() for n,p in self.platforms.items()},"exchanges":{n:e.get_status() for n,e in self.exchanges.items()}}
