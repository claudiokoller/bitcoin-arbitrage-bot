"""
Trading Engine – Core orchestration loop.

Manages the full arbitrage cycle:
  1. Create sell offers on P2P platform with dynamic premium
  2. Buy BTC on exchange when offer is matched
  3. Fund escrow address
  4. Monitor contracts and confirm payments
  5. Auto-reduce premium on stale offers (PATCH live offers)
  6. Track profit per trade with full fee breakdown
"""
import json, logging, threading, time
from datetime import datetime
from core.models import SellOffer, OfferStatus, Platform, Exchange, TradeResult
from core.trade_logger import TradeLogger
from core.pricing import DynamicPricer
log = logging.getLogger("bot.engine")


class SpotPriceProvider:
    """Multi-source BTC spot price provider with fallback chain and cache."""
    _cache = {}  # {currency: (price, timestamp)} — fallback if all sources fail

    @staticmethod
    def _cached(currency, price):
        SpotPriceProvider._cache[currency] = (price, time.time())
        return price

    @staticmethod
    def _get_cached(currency, max_age=300):
        """Return cached price if fresh enough (default 5min), else None.
        Hard limit: reject cache older than 30min to prevent stale-price trades."""
        HARD_MAX_AGE = 1800  # 30 minutes absolute maximum
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
    def get_spot(currency="EUR"):
        """Fetch current BTC spot price in given currency.
        Falls back through multiple APIs: CoinGecko → Kraken → cache."""
        import requests
        providers = {
            "EUR": [
                ("https://api.coingecko.com/api/v3/simple/price",
                 {"ids": "bitcoin", "vs_currencies": currency.lower()}),
                ("https://api.kraken.com/0/public/Ticker",
                 {"pair": f"XXBTZ{currency}"}),
            ],
        }
        for url, params in providers.get(currency, providers["EUR"]):
            try:
                r = requests.get(url, params=params, timeout=10)
                r.raise_for_status()
                data = r.json()
                if "bitcoin" in data:
                    return SpotPriceProvider._cached(currency, data["bitcoin"][currency.lower()])
                for v in data.get("result", {}).values():
                    return SpotPriceProvider._cached(currency, float(v["c"][0]))
            except Exception:
                continue
        cached = SpotPriceProvider._get_cached(currency)
        if cached:
            return cached
        raise ValueError(f"Cannot fetch {currency} spot price")


class TradingEngine:
    def __init__(self, config):
        self.config = config
        self.platforms = {}
        self.exchanges = {}
        self.trade_logger = TradeLogger(config.get("db_path", "trades.db"))
        self.pricer = DynamicPricer(config)
        self.notifier = None
        self.pending_escrows = {}          # {offer_id: {platform, escrow_address, amount_sats, funded, funded_at, premium, buy_data}}
        self._escrow_lock = threading.Lock()
        self._contracted_offers = set()    # Offer IDs with active contracts
        self._buy_data_cache = {}          # Preserved buy_data after offer removed from pending_escrows
        self._recorded_contracts = set()
        self._notified_contracts = set()
        self.daily_volume_sats = 0
        self.paused = False
        self.poll_interval = config.get("poll_interval", 30)
        self.auto_fund = config.get("auto_buy_and_fund", False)
        self.auto_confirm = config.get("auto_confirm_payment", False)

        # Premium auto-reduction
        self._last_stale_check = None

        # Refund monitoring
        self._pending_refunds = {}
        self._last_refund_check = None

        self._start_time = datetime.now()

    def add_platform(self, p):
        self.platforms[p.name] = p

    def add_exchange(self, e):
        self.exchanges[e.name] = e

    def set_notifier(self, n):
        self.notifier = n

    def start(self):
        log.info(f"Platforms: {list(self.platforms.keys())}")
        log.info(f"Exchanges: {list(self.exchanges.keys())}")
        for name, p in self.platforms.items():
            try:
                p.authenticate()
            except Exception as e:
                log.error(f"{name}: auth failed: {e}")
        while True:
            try:
                if not self.paused:
                    self._tick()
            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"Tick error: {e}")
            time.sleep(self.poll_interval)

    def _prune_tracking_sets(self):
        """Prevent unbounded growth of tracking sets."""
        for s, label in ((self._notified_contracts, "_notified_contracts"),
                         (self._recorded_contracts, "_recorded_contracts")):
            if len(s) > 200:
                sorted_ids = sorted(s)
                to_remove = sorted_ids[:-100]
                s -= set(to_remove)
                log.debug(f"Pruned {label}: {len(to_remove)} old entries removed")

    def _tick(self):
        tick_start = time.time()
        for name, platform in self.platforms.items():
            self._process_platform(name, platform)
        self._prune_tracking_sets()
        tick_duration = time.time() - tick_start
        if tick_duration > self.poll_interval:
            log.warning(f"Tick took {tick_duration:.1f}s (> {self.poll_interval}s poll interval)")

    def _process_platform(self, name, platform):
        """Single platform processing cycle (~30s interval)."""
        self._sync_funded_offers(name, platform)
        if self.auto_fund:
            self._fund_escrows(name, platform)
        self._check_trade_requests(name, platform)
        self._check_contracts(name, platform)
        self._check_stale_offers(name, platform)

    # ── Offer Sync ────────────────────────────────────────────────────────

    def _sync_funded_offers(self, name, platform):
        """Sync FUNDED offers into pending_escrows (e.g. after restart)."""
        try:
            active = platform.get_active_offers()
        except Exception:
            return
        for offer in active:
            with self._escrow_lock:
                if offer.id not in self.pending_escrows and offer.id not in self._contracted_offers:
                    try:
                        escrow_info = platform.get_escrow_status(offer.id)
                        funding = escrow_info.get("funding", {})
                        status = funding.get("status", "") if isinstance(funding, dict) else str(funding)
                        if status not in ("FUNDED", "MEMPOOL"):
                            continue
                        # Use real creation date from API (not now()) so premium reduction timer is accurate
                        real_date = offer.raw_data.get("publishingDate") or offer.raw_data.get("creationDate") or datetime.now().isoformat()
                        if real_date.endswith("Z"):
                            real_date = real_date.replace("Z", "+00:00")
                        self.pending_escrows[offer.id] = {
                            "platform": name,
                            "escrow_address": escrow_info.get("escrow", ""),
                            "amount_sats": offer.max_sats,
                            "funded": True,
                            "funded_at": real_date,
                            "premium": offer.premium_pct,
                        }
                        log.info(f"{name}: synced offer {offer.id}")
                    except Exception:
                        pass

    # ── Escrow Funding ────────────────────────────────────────────────────

    def _fund_escrows(self, name, platform):
        """Buy BTC on exchange and withdraw to escrow address."""
        with self._escrow_lock:
            to_fund = {oid: info for oid, info in self.pending_escrows.items()
                       if info["platform"] == name and not info.get("funded") and not info.get("funding_in_progress")}
        if not to_fund:
            return
        exchange = self._get_best_exchange()
        if not exchange:
            return
        for oid, info in to_fund.items():
            try:
                addr = info.get("escrow_address", "")
                if not addr:
                    addr = platform.get_escrow_address(oid)
                    info["escrow_address"] = addr
                spot = exchange.get_spot_price()
                fiat = (info["amount_sats"] / 1e8) * spot * 1.005  # 0.5% buffer
                result = exchange.buy_and_withdraw(fiat, addr)
                info["funded"] = True
                info["funded_at"] = datetime.now().isoformat()
                log.info(f"{name}: funded {oid[:12]}")
            except Exception as e:
                log.error(f"{name}: fund {oid[:12]}: {e}")

    # ── Trade Request Matching ────────────────────────────────────────────

    def _check_trade_requests(self, name, platform):
        """Check for incoming trade requests and auto-accept with payment data."""
        pconfig = self.config.get("platforms", {}).get(name, {})
        raw = pconfig.get("payment_data_raw", {})

        # Build payment data map (method → raw payment data for hashing/encryption)
        payment_data_map = self._build_payment_data_map(raw)

        with self._escrow_lock:
            funded = {oid: info for oid, info in self.pending_escrows.items()
                      if info["platform"] == name and info.get("funded")}

        for oid, info in funded.items():
            try:
                trade_requests = platform.check_trade_requests(oid)
                if not trade_requests:
                    continue
                tr = trade_requests[0]
                method = tr.get("paymentMethod", "")
                raw_data = payment_data_map.get(method, "")
                if not raw_data:
                    log.warning(f"{name}: No payment data for {method}")
                    continue
                buyer_id = tr.get("userId", "")
                platform.accept_trade_request(oid, buyer_id, method, raw_data, trade_request_data=tr)
                log.info(f"{name}: ACCEPTED {buyer_id[:12]} via {method}")
                if self.notifier:
                    self.notifier.notify_match(oid, buyer_id)
            except Exception as e:
                log.warning(f"{name}: match check {oid[:12]}: {e}")
                # Only remove on 401 if already funded (unfunded offers get 401 normally)
                if "401" in str(e) and info.get("funded"):
                    self._contracted_offers.add(oid)
                    with self._escrow_lock:
                        self.pending_escrows.pop(oid, None)

    def _build_payment_data_map(self, raw):
        """Build payment data map for trade request acceptance.
        Each payment method has its own data format for hashing."""
        # Implementation handles: twint (phone), revolut/wise (JSON userName),
        # sepa/instantSepa (IBAN), etc.
        # Details omitted for security
        raise NotImplementedError("Payment data map construction - see private repo")

    # ── Contract Monitoring ───────────────────────────────────────────────

    def _check_contracts(self, name, platform):
        """Monitor contract lifecycle: payment → confirmation → completion."""
        # Prune _contracted_offers to prevent unbounded growth
        if len(self._contracted_offers) > 100:
            sorted_ids = sorted(self._contracted_offers, key=lambda x: int(x) if x.isdigit() else 0)
            self._contracted_offers = set(sorted_ids[-50:])
        try:
            contracts = platform.get_contracts()
        except Exception:
            return

        for c in contracts:
            offer_id = (c.id.split("-")[0] if "-" in c.id else getattr(c, "offer_id", None)) or ""
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
            elif c.status == OfferStatus.COMPLETED:
                if c.id not in self._recorded_contracts:
                    self._recorded_contracts.add(c.id)
                    self._record_trade(name, c)

    # ── Premium Auto-Reduction ────────────────────────────────────────────

    @staticmethod
    def _parse_funded_at(funded_at_str):
        """Parse funded_at string to naive local datetime (handles both UTC ISO and local)."""
        dt = datetime.fromisoformat(funded_at_str)
        if dt.tzinfo is not None:
            utc_ts = dt.timestamp()
            dt = datetime.fromtimestamp(utc_ts)
        return dt

    def _check_stale_offers(self, name, platform):
        """Reduce premium on funded offers without match after X hours.
        Uses PATCH API to update premium directly on live offers.
        Also cleans up very old pending_escrows entries (>7 days)."""
        now = datetime.now()
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
                log.info(f"{name}: removed stale pending_escrow {oid[:12]} (>7 days old)")

        pconfig = self.config.get("platforms", {}).get(name, {})
        reduction_hours = pconfig.get("premium_reduction_hours", 24)
        reduction_step = pconfig.get("premium_reduction_step", 0.5)
        floor = pconfig.get("premium_floor", 4.0)

        with self._escrow_lock:
            funded = {oid: info for oid, info in self.pending_escrows.items()
                      if info["platform"] == name and info.get("funded") and info.get("funded_at")}

        for oid, info in funded.items():
            try:
                funded_at = self._parse_funded_at(info["funded_at"])
                hours_waiting = (now - funded_at).total_seconds() / 3600
                if hours_waiting < reduction_hours:
                    continue
                current = info.get("premium", 6.0)
                new_premium = max(round(current - reduction_step, 1), floor)
                if new_premium >= current:
                    continue

                if hasattr(platform, "update_premium") and platform.update_premium(oid, new_premium):
                    info["premium"] = new_premium
                    info["funded_at"] = now.isoformat()  # reset timer
                    log.info(f"{name}: {oid[:12]} premium {current}% -> {new_premium}%")
                    if self.notifier:
                        self.notifier._send(
                            f"<b>Premium gesenkt</b>\n"
                            f"Offer {oid}: {current}% → {new_premium}%\n"
                            f"Kein Match seit {hours_waiting:.0f}h"
                        )
            except Exception as e:
                log.warning(f"{name}: stale check {oid[:12]}: {e}")

    # ── Trade Recording ───────────────────────────────────────────────────

    def _record_trade(self, pname, contract):
        """Record completed trade with full fee breakdown.

        Fee components:
        - Exchange trading fee (e.g. Kraken 0.26%)
        - Withdrawal fee: exchange → hot wallet (fixed per exchange)
        - Funding fee: hot wallet → escrow (on-chain TX fee)
        - Platform fee: 0 for seller (Peach 2% is paid by buyer)
        """
        currency = getattr(contract, "currency", "EUR")
        try:
            spot_now = SpotPriceProvider.get_spot(currency)
        except Exception:
            spot_now = 0

        btc = contract.amount_sats / 1e8
        sell_price = contract.price_fiat

        # Look up buy data (preserves actual Kraken buy price across escrow lifecycle)
        buy_data = self._lookup_buy_data(contract)

        KRAKEN_WITHDRAWAL_FEE_BTC = 0.000015

        if buy_data.get("fiat_spent"):
            buy_price = buy_data["fiat_spent"]
            efee = buy_data.get("exchange_fee", 0)
            spot_at_buy = buy_data.get("spot_at_buy", 0)
            funding_fee = buy_data.get("funding_fee_sats", 0) / 1e8 * spot_now
        else:
            buy_price = btc * spot_now
            efee = buy_price * 0.0026
            spot_at_buy = spot_now
            funding_fee = 750 / 1e8 * spot_now  # ~150 vB × 5 sat/vB

        withdrawal_fee = KRAKEN_WITHDRAWAL_FEE_BTC * spot_now if spot_now else 0
        net = sell_price - buy_price - efee - withdrawal_fee - funding_fee

        self.trade_logger.log_trade(
            platform=pname, contract_id=contract.id,
            amount_sats=contract.amount_sats,
            buy_price=buy_price, sell_price=sell_price,
            currency=currency, exchange_fee=efee,
            withdrawal_fee=withdrawal_fee, funding_fee=funding_fee,
            net_profit=net, payment_method=contract.payment_method,
            spot_at_buy=spot_at_buy, spot_at_sell=spot_now,
        )
        log.info(f"{pname}: COMPLETE {contract.id[:12]} profit={net:.2f} {currency}")

    def _lookup_buy_data(self, contract):
        """Find preserved buy data from pending_escrows or cache."""
        offer_id = getattr(contract, "offer_id", "") or \
                   (contract.id.split("-")[0] if "-" in contract.id else "")
        for oid in (offer_id, contract.id):
            if oid:
                info = self.pending_escrows.get(oid, {})
                buy_data = info.get("buy_data", {}) or self._buy_data_cache.pop(oid, {})
                if buy_data:
                    return buy_data
        return {}

    def _get_best_exchange(self):
        best, best_price = None, float("inf")
        for ex in self.exchanges.values():
            try:
                price = ex.get_spot_price()
                if price < best_price:
                    best_price, best = price, ex
            except Exception:
                pass
        return best

    def get_exchange_by_currency(self, currency):
        """Get exchange instance that trades in the given fiat currency."""
        for ex in self.exchanges.values():
            if hasattr(ex, 'get_fiat_currency') and ex.get_fiat_currency() == currency:
                return ex
        return None

    def get_status(self):
        uptime = datetime.now() - self._start_time
        h, m = divmod(int(uptime.total_seconds()) // 60, 60)
        d, h = divmod(h, 24)
        return {
            "paused": self.paused,
            "uptime": f"{d}d {h}h {m}m" if d else f"{h}h {m}m",
            "daily_volume_sats": self.daily_volume_sats,
            "pending_escrows": len(self.pending_escrows),
            "funded_escrows": sum(1 for e in self.pending_escrows.values() if e.get("funded")),
        }
