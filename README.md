# Bitcoin Arbitrage Trading Bot

Semi-automated Bitcoin arbitrage between centralized exchanges and P2P platforms.

Buy BTC at spot price on an exchange, sell at a premium on a P2P marketplace (the platform currently caps sell offers at +6%). Offer creation and premium setting are done manually — trade matching, payment handling, and escrow release are automated.

**[Architecture Diagram](https://claudiokoller.github.io/bitcoin-arbitrage-bot/architecture-diagram.html)**

## Trade Cycle

1. **Create sell offer** on P2P platform with manual premium (e.g. +7%)
2. **Buy BTC** on exchange at spot price (CHF/EUR/USD/USDT)
3. **Fund escrow** — withdraw to hot wallet, then on-chain TX to the escrow address (verified against our own key first)
4. **Match** — auto-accept trade requests with encrypted payment data (PGP)
5. **Payment** — buyer pays via Twint/SEPA/Revolut/Wise/Skrill/N26/Paysera/USDT
6. **Release** — verify the release PSBT, then sign it (taproot key path, or legacy 2-of-2)
7. **Auto-reduce premium** — PATCH live offers if no match after 24h

## Features

- **Multi-currency**: CHF, EUR, USD, USDT support
- **Multi-payment**: Twint, SEPA, SEPA Instant, Revolut, Wise, Skrill, N26, Paysera, USDT (Solana/Arbitrum/Ethereum)
- **HD escrow keys**: BIP32 derivation per offer (`m/84'/0'/0'/{offerId}'`)
- **Single-sig taproot escrow**: BIP341/340 key-path escrow (`escrowVersion 2`) with the legacy 2-of-2 P2WSH path kept for older contracts
- **Cap-aware offer sizing**: offer sizes are derived from the platform's CHF cap, converted to sats at the current spot price each cycle, so they stay maximal as the BTC price moves
- **PGP encryption**: Symmetric key exchange for payment data
- **Auto premium reduction**: Live PATCH on stale offers (no cancel/refund cycle)
- **Auto buy-escrow**: Buys BTC and creates a funded offer automatically on a configurable interval (`/auto [premium%]`)
- **Dual fill detection**: Order polling + balance change fallback
- **Profit tracking**: Full fee breakdown (exchange, withdrawal, funding, platform)
- **Telegram bot**: Complete remote control with inline keyboards
- **Market scanner**: Competitive analysis with premium recommendations
- **Web dashboard**: Real-time P&L, trade history, payment method breakdown, market monitor

## Project Structure

```
├── core/
│   ├── engine.py          # Main trading loop (~30s tick)
│   ├── models.py          # Data models (SellOffer, Contract, etc.)
│   ├── hd_keys.py         # BIP32/BIP39 key derivation (pure Python)
│   ├── taproot.py         # BIP340/341/086: tweak, bech32m, sighash, schnorr
│   ├── offer_sizing.py    # Offer sizes derived from the platform's CHF cap
│   ├── pricing.py         # Dynamic premium calculation
│   └── trade_logger.py    # SQLite trade history
├── exchanges/
│   ├── base.py            # Exchange base class
│   └── kraken.py          # Kraken API (HMAC-SHA512 auth)
├── platforms/
│   ├── base.py            # Platform base class
│   └── peach.py           # Peach Bitcoin API (v1 + v069)
├── notifications/
│   └── telegram_bot.py    # Telegram notifications + commands
├── dashboard.py           # Web dashboard (Flask)
├── run.py                 # Entry point
├── config.example.json    # Configuration template
└── architecture-diagram.html  # Architecture diagram
```

## Setup

```bash
pip install requests python-telegram-bot coincurve pgpy flask
cp config.example.json config.json
# Edit config.json with your API keys, mnemonic, payment data
python run.py
```

## Configuration

See `config.example.json` for all options. Key settings:

- **Exchange**: API key/secret, trading pair, withdrawal key
- **P2P Platform**: Private key (secp256k1), mnemonic (BIP39), PGP keypair
- **Payment methods**: Per-currency method list (CHF/EUR/USD/USDT)
- **Premium**: Base premium, floor, auto-reduction interval. The platform enforces its own bounds — currently −5% to +6%; anything above is rejected outright.
- **Auto buy-escrow**: Interval, offer sizing (`size_mode`, `cap_fractions`, `max_offer_chf`, `max_offer_sats`), fixed premium, excluded methods
- **Telegram**: Bot token + chat ID for notifications

## Key Design Decisions

- **v069 API for trade requests**: The v1 matches endpoint often returns empty. The undocumented v069 endpoint reliably returns incoming trade requests.
- **HD key derivation**: Each offer gets a unique escrow key derived from the mnemonic, matching the P2P app's derivation path for compatibility.
- **Live premium PATCH**: Instead of cancelling stale offers (which triggers on-chain refund), premium is reduced via PATCH on the live offer.
- **Dual fill detection**: Exchange order queries can be slow. After 15s, the bot also checks balance changes as a fallback to detect filled orders faster.
- **Buy data preservation**: Actual exchange buy price is preserved through the full escrow lifecycle for accurate profit calculation.
- **Escrow address verification**: A single-sig escrow address is a pure function of our escrow key, so the address returned by the platform is re-derived locally and rejected on mismatch. Under the legacy 2-of-2 escrow a wrong address merely produced an unspendable output; with single-sig it would hand the coins to someone else.
- **Release PSBT verification before signing**: The release PSBT must spend our funding transaction and pay the buyer's release address a non-zero amount. A legacy PSBT was harmless without the platform's counter-signature — a single-sig one is not, since our signature alone is sufficient to move the funds.
- **Offer sizes derived from the CHF cap**: Three denominations are in play — the platform caps an offer in *Swiss francs* (a regulatory limit), the rotation is configured in *fiat*, and the offer actually validated is in *sats*. A static fiat list silently loses its largest entries when BTC falls and leaves headroom unused when it rises. Holding the cap as a sats constant has the same defect one level up: the same number is a different CHF amount every day, so the top sizes start getting rejected as BTC rises. Resolving the cap from CHF each cycle keeps the largest offer at ~98% of what the platform allows at any price and holds its *fiat* value steady. Larger offers also mean fewer bank transfers for the same volume, which matters beyond fees.
- **A raised cap needs a floor**: Every rotation size derives from the cap, so a cap configured higher than the platform actually grants puts *all* of them out of range and the step-down would create no offer at all. One candidate sized to the previous cap is always kept, which bounds that failure to a few rejected API calls that also record the true limit.
- **Offer cooldown**: Auto buy-escrow enforces a configurable minimum between offer creations (`min_offer_interval_sec`) to prevent rapid re-triggering when an exchange withdrawal arrives faster than the check interval.

## Disclaimer

This bot is actively used in production. The code here is shared as a portfolio showcase and for educational purposes — it is not a turnkey solution and will not run unattended as published.

**What is complete and real:** the single-sig taproot escrow implementation (BIP340/341/086 — tweak, bech32m, sighash, schnorr), BIP32/BIP39 key derivation, offer sizing, the marketplace and exchange clients, the data model, the trade database and the dashboard.

**What is not published:** the operative trading logic inside `core/engine.py` — offer creation and sizing decisions, escrow funding coordination, trade-request matching and auto-accept, and premium maintenance on stale offers. Those methods are kept as documented signatures so the architecture and control flow remain readable, but they raise `NotImplementedError`. Payment-data handling and the PGP encryption flow are likewise omitted.

Use at your own risk.
