# Bitcoin Arbitrage Bot

Automated Bitcoin arbitrage between centralized exchanges (Kraken) and P2P platforms (Peach Bitcoin).

Buy BTC at spot price on Kraken, sell at a premium on Peach P2P — fully automated from offer creation to escrow release.

**[Interactive Architecture Diagram](https://claudiokoller.github.io/bitcoin-arbitrage-bot/architecture-diagram.html)**

## Trade Cycle

1. **Create sell offer** on Peach with dynamic premium (e.g. +6%)
2. **Buy BTC** on Kraken at spot price
3. **Fund escrow** — withdraw to hot wallet, then on-chain TX to escrow address
4. **Match** — auto-accept trade requests with encrypted payment data (PGP)
5. **Payment** — buyer pays via Twint/SEPA/Revolut/Wise
6. **Release** — sign PSBT to release escrow after payment confirmation
7. **Auto-reduce premium** — PATCH live offers if no match after 24h

## Features

- **Multi-currency**: CHF, EUR, USD support
- **Multi-payment**: Twint, SEPA, SEPA Instant, Revolut, Wise
- **HD escrow keys**: BIP32 derivation matching Peach app (`m/84'/0'/0'/{offerId}'`)
- **PGP encryption**: Symmetric key exchange for payment data
- **Auto premium reduction**: Live PATCH on stale offers (no cancel/refund cycle)
- **Dual fill detection**: Kraken order polling + balance change fallback
- **Profit tracking**: Full fee breakdown (exchange, withdrawal, funding, platform)
- **Telegram bot**: Complete remote control with inline keyboards
- **Market scanner**: Competitive analysis with premium recommendations

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
├── run.py                 # Entry point
├── config.example.json    # Configuration template
└── architecture-diagram.html  # Interactive architecture diagram
```

## Setup

```bash
pip install requests python-telegram-bot coincurve pgpy
cp config.example.json config.json
# Edit config.json with your API keys, mnemonic, payment data
python run.py
```

## Configuration

See `config.example.json` for all options. Key settings:

- **Kraken**: API key/secret, trading pair, withdrawal key
- **Peach**: Private key (secp256k1), mnemonic (BIP39), PGP keypair
- **Payment methods**: Per-currency payment method configuration
- **Premium**: Base premium, floor, auto-reduction interval
- **Telegram**: Bot token + chat ID for notifications

## Key Design Decisions

- **v069 API for trade requests**: The v1 matches endpoint often returns empty. The undocumented v069 endpoint reliably returns incoming trade requests.
- **HD key derivation**: Each offer gets a unique escrow key derived from the mnemonic, matching Peach app's derivation path for compatibility.
- **Live premium PATCH**: Instead of cancelling stale offers (which triggers on-chain refund), premium is reduced via PATCH on the live offer.
- **Dual fill detection**: Kraken's QueryOrders can be slow. After 15s, the bot also checks balance changes as a fallback to detect filled orders faster.
- **Buy data preservation**: Actual Kraken buy price is preserved through the full escrow lifecycle for accurate profit calculation.

## Disclaimer

This is a showcase of the bot's architecture. Some implementation details (payment data handling, PGP encryption flow, escrow funding) are intentionally omitted. This is not a turnkey solution.

Use at your own risk. This software is provided as-is for educational purposes.
