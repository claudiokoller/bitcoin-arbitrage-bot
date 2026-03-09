import logging, time
from dataclasses import dataclass, field
import requests
log = logging.getLogger("bot.pricing")

@dataclass
class PricingConfig:
    enabled: bool = True
    floor_pct: float = 4.0
    ceiling_pct: float = 10.0
    undercut_by: float = 0.5
    scan_interval: int = 300
    min_competitors: int = 1
    ignore_outliers_below: float = 2.0
    smooth_factor: float = 0.5

@dataclass
class MarketSnapshot:
    platform: str = ""
    timestamp: float = 0
    competitor_premiums: list = field(default_factory=list)
    lowest_premium: float = 0
    median_premium: float = 0
    num_offers: int = 0
    recommended_premium: float = 0
    reason: str = ""

class DynamicPricer:
    def __init__(self, global_config):
        self.configs = {}
        self.snapshots = {}
        self.current_premiums = {}
        for pname, pcfg in global_config.get("platforms", {}).items():
            pricing = pcfg.get("dynamic_pricing", {})
            if pricing:
                self.configs[pname] = PricingConfig(
                    enabled=pricing.get("enabled",True), floor_pct=pricing.get("floor_pct",4.0),
                    ceiling_pct=pricing.get("ceiling_pct",10.0), undercut_by=pricing.get("undercut_by",0.5),
                    scan_interval=pricing.get("scan_interval",300), min_competitors=pricing.get("min_competitors",1),
                    ignore_outliers_below=pricing.get("ignore_outliers_below",2.0), smooth_factor=pricing.get("smooth_factor",0.5))
            else:
                self.current_premiums[pname] = pcfg.get("target_premium", 6.0)
    def get_premium(self, platform_name, platform=None):
        cfg = self.configs.get(platform_name)
        if not cfg or not cfg.enabled:
            return self.current_premiums.get(platform_name, 6.0)
        snapshot = self.snapshots.get(platform_name)
        now = time.time()
        if not snapshot or (now - snapshot.timestamp) > cfg.scan_interval:
            snapshot = self._scan_market(platform_name, platform)
            self.snapshots[platform_name] = snapshot
        new_premium = self._calculate_premium(platform_name, snapshot, cfg)
        old = self.current_premiums.get(platform_name, new_premium)
        smoothed = old * cfg.smooth_factor + new_premium * (1 - cfg.smooth_factor)
        smoothed = round(smoothed, 1)
        smoothed = max(cfg.floor_pct, min(cfg.ceiling_pct, smoothed))
        if smoothed != old:
            log.info(f"{platform_name}: premium {old}% -> {smoothed}%")
        self.current_premiums[platform_name] = smoothed
        return smoothed
    def _calculate_premium(self, name, snap, cfg):
        if snap.num_offers < cfg.min_competitors:
            return cfg.ceiling_pct
        lowest = snap.lowest_premium
        target = lowest - cfg.undercut_by
        if target < cfg.floor_pct:
            snap.reason = f"floor (cant undercut {lowest}%)"
            return cfg.floor_pct
        snap.reason = f"undercut {lowest}% by {cfg.undercut_by}%"
        return target
    def _scan_market(self, platform_name, platform=None):
        snap = MarketSnapshot(platform=platform_name, timestamp=time.time())
        try:
            if platform_name == "peach": premiums = self._scan_peach(platform)
            else: premiums = []
            cfg = self.configs.get(platform_name, PricingConfig())
            premiums = [p for p in premiums if p >= cfg.ignore_outliers_below]
            snap.competitor_premiums = sorted(premiums)
            snap.num_offers = len(premiums)
            if premiums:
                snap.lowest_premium = min(premiums)
                snap.median_premium = sorted(premiums)[len(premiums)//2]
        except Exception as e:
            log.warning(f"{platform_name}: scan failed: {e}")
            snap.reason = f"scan error: {e}"
        return snap
    def _scan_peach(self, platform=None):
        premiums = []
        seen = set()
        searches = [
            ("CHF", "twint"), ("CHF", "revolut"), ("CHF", "wise"),
            ("EUR", "sepa"), ("EUR", "instantSepa"), ("EUR", "revolut"), ("EUR", "wise"),
        ]
        for curr, method in searches:
            try:
                if platform and hasattr(platform, 'session') and hasattr(platform, 'base_url_v069'):
                    r = platform.session.get(f"{platform.base_url_v069}/sellOffer",
                        params={"currency": curr, "paymentMethod": method}, timeout=15)
                else:
                    r = requests.get("https://api.peachbitcoin.com/v069/sellOffer",
                        params={"currency": curr, "paymentMethod": method}, timeout=15)
                r.raise_for_status()
                data = r.json()
                raw = data.get("offers", data) if isinstance(data, dict) else data
                for o in raw:
                    oid = str(o.get("id", ""))
                    if oid in seen: continue
                    seen.add(oid)
                    p = o.get("premium")
                    if p is not None and isinstance(p, (int, float)):
                        premiums.append(float(p))
            except Exception as e:
                log.debug(f"Peach scan {curr}/{method}: {e}")
        return premiums
    def get_snapshot(self, platform_name):
        return self.snapshots.get(platform_name)
    def get_all_premiums(self):
        return dict(self.current_premiums)
    def force_rescan(self, platform_name, platform=None):
        snap = self._scan_market(platform_name, platform)
        self.snapshots[platform_name] = snap
        return snap
