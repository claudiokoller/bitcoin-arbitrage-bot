"""
Offer sizing for auto_buy_escrow.

Peach caps a sell offer in *Swiss francs* — the cap comes from a Swiss
regulatory limit, not from a bitcoin amount. Everything else here is
denominated differently: the rotation amounts are configured in *euro* and the
offer Peach actually validates is in *sats*. All three drift apart whenever the
BTC price moves. Since a larger offer means fewer SEPA transfers for the same
volume — and every transfer carries bank-closure risk — giving away headroom is
not neutral, and overshooting is worse: Peach rejects the offer outright with
FORM_INVALID["amount"].

So the cap is resolved per cycle: `max_offer_chf` is converted to sats at the
current BTC/CHF spot (see `resolve_cap_sats`). `cap_fraction` mode then
expresses the rotation as fractions of that cap and converts to euro at the
current BTC/EUR spot. The offers stay as large as Peach permits at any price
while keeping a spread across sizes. With `min_offer_chf` the fractions are
derived instead: an even ladder from just under the cap down to that CHF floor
(see `resolve_fractions`).

`max_offer_sats` remains as the fallback for a CHF price-feed outage, and as the
last *known-good* cap: raising `max_offer_chf` to a limit Peach has not actually
granted yet would otherwise make every offer fail, so the buy path keeps one
candidate that fit under the old cap. See the step-down in
`_execute_buy_escrow`.

The legacy `fixed` mode (a plain euro list) is still honoured so existing configs
keep working unchanged.
"""
import math

# Peach's per-offer cap before the 2026-09-17 raise, in sats. Only a last-resort
# default: the real cap is `max_offer_chf`, and `max_offer_sats` overrides this.
LEGACY_CAP_SATS = 800_000


def resolve_cap_sats(auto_cfg, spot_chf=None):
    """Peach's per-offer cap expressed in sats at the current price.

    A sats constant cannot express this cap: it silently blocks the top sizes
    when BTC rises and gives away headroom when BTC falls. Falls back to
    `max_offer_sats` when no CHF cap is configured or the CHF spot is
    unavailable — never guesses, so a price-feed hiccup cannot inflate the cap
    past what Peach accepts.
    """
    cap_chf = auto_cfg.get("max_offer_chf")
    fallback = int(auto_cfg.get("max_offer_sats", LEGACY_CAP_SATS))
    if cap_chf and spot_chf:
        return int(cap_chf / spot_chf * 1e8)
    return fallback


def resolve_fractions(auto_cfg):
    """The fractions of the cap to size offers at, largest first.

    With `min_offer_chf` set, the ladder runs from just under the cap down to
    that floor in `size_steps` even steps. The floor is a fixed CHF amount on
    purpose: expressed as a fraction it would drift upward whenever Peach
    raises the cap and `max_offer_chf` follows. It takes precedence over
    `cap_fractions`, which remains for hand-picked spreads.
    """
    top = 0.98  # headroom for price movement between sizing and Peach's check
    cap_chf = auto_cfg.get("max_offer_chf")
    floor_chf = auto_cfg.get("min_offer_chf")
    if cap_chf and floor_chf:
        low = floor_chf / cap_chf
        steps = max(int(auto_cfg.get("size_steps", 4)), 1)
        if low >= top or steps == 1:
            return [top]
        step = (top - low) / (steps - 1)
        return [round(top - i * step, 4) for i in range(steps)]
    return auto_cfg.get("cap_fractions") or [0.98, 0.96, 0.94, 0.92, 0.90, 0.88, 0.86, 0.84]


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


def effective_amounts(auto_cfg, spot_eur, withdraw_fee_sats, min_amount_sats, spot_chf=None):
    """The euro rotation amounts to use this cycle, largest last.

    Returns [] when sizing cannot be computed (no spot price), so callers fall
    back to their configured list rather than guessing.
    """
    mode = auto_cfg.get("size_mode", "fixed")
    configured = [a for a in auto_cfg.get("amounts", []) if isinstance(a, (int, float))]

    if mode != "cap_fraction" or not spot_eur:
        return sorted(configured)

    cap = resolve_cap_sats(auto_cfg, spot_chf)
    fractions = resolve_fractions(auto_cfg)
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


def describe(auto_cfg, spot_eur, withdraw_fee_sats, min_amount_sats, spot_chf=None):
    """[(eur, sats, pct_of_cap)] for logging and the /sizes command."""
    cap = resolve_cap_sats(auto_cfg, spot_chf)
    rows = []
    for eur in effective_amounts(auto_cfg, spot_eur, withdraw_fee_sats, min_amount_sats, spot_chf):
        s = sats_for_eur(eur, spot_eur, withdraw_fee_sats, min_amount_sats)
        rows.append((eur, s, 100.0 * s / cap if cap else 0.0))
    return rows
