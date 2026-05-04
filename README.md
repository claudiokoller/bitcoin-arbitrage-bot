# Bitcoin Arbitrage Bot

Semi-automated Bitcoin arbitrage between centralized exchanges and P2P platforms.

Buy BTC at spot price on an exchange, sell at a 3–6% premium on a P2P marketplace. Offer creation and premium setting are done manually — trade matching, payment handling, and escrow release are automated.

**[Architecture Diagram](https://claudiokoller.github.io/bitcoin-arbitrage-bot/architecture-diagram.html)**

## Trade Cycle

1. **Create sell offer** on P2P platform with manual premium (e.g. +6%)
2. **Buy BTC** on exchange at spot price (CHF/EUR/USD/USDT)
3. **Fund escrow** — withdraw to hot wallet, then on-chain TX to escrow address
4. **Match** — auto-accept trade requests with encrypted payment data (PGP)
5. **Payment** — buyer pays via Twint/SEPA/Revolut/Wise/Skrill/N26/Paysera/USDT
6. **Release** — sign PSBT to release escrow after payment confirmation
7. **Auto-reduce premium** — PATCH live offers if no match after 24h

## Features

- **Multi-currency**: CHF, EUR, USD, USDT support
- **Multi-payment**: Twint, SEPA, SEPA Instant, Revolut, Wise, Skrill, N26, Paysera, USDT (Solana/Arbitrum/Ethereum)
- **HD escrow keys**: BIP32 derivation per offer (`m/84'/0'/0'/{offerId}'`)
- **PGP encryption**: Symmetric key exchange for payment data
- **Auto premium reduction**: Live PATCH on stale offers (no cancel/refund cycle)
- **Auto buy-escrow**: Every 30 min, buys BTC and creates a funded offer automatically (`/auto [premium%]`)
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
- **Premium**: Base premium (typically 3–6%), floor, auto-reduction interval
- **Auto buy-escrow**: Interval (effective 30 min), amounts, fixed premium, excluded methods
- **Telegram**: Bot token + chat ID for notifications

## Key Design Decisions

- **v069 API for trade requests**: The v1 matches endpoint often returns empty. The undocumented v069 endpoint reliably returns incoming trade requests.
- **HD key derivation**: Each offer gets a unique escrow key derived from the mnemonic, matching the P2P app's derivation path for compatibility.
- **Live premium PATCH**: Instead of cancelling stale offers (which triggers on-chain refund), premium is reduced via PATCH on the live offer.
- **Dual fill detection**: Exchange order queries can be slow. After 15s, the bot also checks balance changes as a fallback to detect filled orders faster.
- **Buy data preservation**: Actual exchange buy price is preserved through the full escrow lifecycle for accurate profit calculation.
- **30 min offer cooldown**: Auto buy-escrow enforces a 30-minute minimum between offer creations to prevent rapid re-triggering when a Kraken withdrawal arrives faster than the check interval.

## Disclaimer

This bot is actively used in production. Some implementation details (payment data handling, PGP encryption flow, escrow funding) are intentionally omitted from this public repository. This is not a turnkey solution.

The code is shared for educational purposes and as a portfolio showcase. Use at your own risk.
