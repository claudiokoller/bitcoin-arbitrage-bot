"""
Offer sizing for auto_buy_escrow.

Peach caps a sell offer at a fixed number of *sats* (800,000 since the
2026-09 change), but the rotation amounts are configured in *euro*. Those two
drift apart whenever the BTC price moves: a static euro list silently loses its
top entries when BTC falls, and leaves the cap unused when BTC rises. Since a
larger offer means fewer SEPA transfers for the same volume — and every transfer
carries bank-closure risk — giving away headroom is not neutral.

`cap_fraction` mode therefore expresses the rotation as fractions of the sats
cap and converts to euro at the current spot each cycle. The offers then stay as
large as Peach permits at any price, while keeping a spread across sizes.

The legacy `fixed` mode (a plain euro list) is still honoured so existing configs
keep working unchanged.
"""
import math


def sats_for_eur(eur, spot_eur, withdraw_fee_sats, min_amount_sats):
    """Sats an offer of `eur` ends up with — mirrors _run_auto_buy_escrow.

    The 0.99 factor is the exchange-side haircut; the withdrawal fee is paid out
    of the purchased amount.
    """
    gross = int((eur / spot_eur) * 1e8)
    return max(int(gross * 0.99) - withdraw_fee_sats, min_amount_sats)


def eur_for_sats(target_sats, spot_eur, withdraw_fee_sats):
    """Inverse of sats_for_eur, rounded down to whole euro.

    Rounding down matters: rounding up could push the resulting offer one sat
    over Peach's cap, which is rejected outright.
    """
    gross = (target_sats + withdraw_fee_sats) / 0.99
    return int(math.floor(gross / 1e8 * spot_eur))


def effective_amounts(auto_cfg, spot_eur, withdraw_fee_sats, min_amount_sats):
    """The euro rotation amounts to use this cycle, largest last.

    Returns [] when sizing cannot be computed (no spot price), so callers fall
    back to their configured list rather than guessing.
    """
    mode = auto_cfg.get("size_mode", "fixed")
    configured = [a for a in auto_cfg.get("amounts", []) if isinstance(a, (int, float))]

    if mode != "cap_fraction" or not spot_eur:
        return sorted(configured)

    cap = auto_cfg.get("max_offer_sats", 800_000)
    fractions = auto_cfg.get("cap_fractions") or [0.98, 0.96, 0.94, 0.92, 0.90, 0.88, 0.86, 0.84]
    floor_eur = auto_cfg.get("min_offer_eur", 0)

    out = []
    for frac in fractions:
        target = int(cap * frac)
        eur = eur_for_sats(target, spot_eur, withdraw_fee_sats)
        # Guard against rounding: never emit an amount that exceeds the cap.
        while eur > 0 and sats_for_eur(eur, spot_eur, withdraw_fee_sats, min_amount_sats) > cap:
            eur -= 1
        if eur >= floor_eur and eur > 0:
            out.append(eur)

    # Distinct amounts only — at a low BTC price two fractions can collapse onto
    # the same euro value, which would waste a rotation slot.
    return sorted(set(out))


def describe(auto_cfg, spot_eur, withdraw_fee_sats, min_amount_sats):
    """[(eur, sats, pct_of_cap)] for logging and the /sizes command."""
    cap = auto_cfg.get("max_offer_sats", 800_000)
    rows = []
    for eur in effective_amounts(auto_cfg, spot_eur, withdraw_fee_sats, min_amount_sats):
        s = sats_for_eur(eur, spot_eur, withdraw_fee_sats, min_amount_sats)
        rows.append((eur, s, 100.0 * s / cap if cap else 0.0))
    return rows
