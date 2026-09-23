# Bitcoin-Arbitrage-Bot

Ein semi-autonomer Arbitrage-Bot, der Bitcoin auf einer Börse (Kraken) kauft und auf einem Peer-to-Peer-Marktplatz (Peach) mit Aufpreis an Privatpersonen weiterverkauft. Gesteuert wird er über Telegram, dazu gibt es ein kleines Web-Dashboard.

**[Architekturdiagramm](https://claudiokoller.github.io/bitcoin-arbitrage-bot/architecture-diagram.html)**

## Die Idee in einfachen Worten

Auf P2P-Marktplätzen kaufen Leute Bitcoin direkt von anderen Personen, ohne Konto bei einer Börse. Dafür zahlen sie einen Aufpreis gegenüber dem Börsenkurs, typischerweise ein paar Prozent. Der Bot nutzt diese Differenz:

1. BTC auf Kraken zum Börsenkurs kaufen
2. Auf Peach ein Verkaufsangebot mit Aufpreis erstellen (die Plattform erlaubt aktuell höchstens +6 %)
3. Die BTC in ein Escrow einzahlen – ein Treuhandkonto auf der Blockchain, damit der Käufer sicher sein kann, dass die Coins da sind
4. Ein Käufer nimmt das Angebot an und überweist den Betrag (Twint, SEPA, Revolut, Wise …)
5. Sobald das Geld eingegangen ist, werden die BTC aus dem Escrow an den Käufer freigegeben

Der Gewinn ist der Aufpreis abzüglich Gebühren (Börse, Auszahlung, Blockchain-Transaktion, Plattform).

Angebote lassen sich per Telegram-Befehl von Hand anstossen oder im Auto-Modus in einem festen Intervall erstellen. Den Zahlungseingang prüfe ich selbst: Meldet ein Käufer, dass er bezahlt hat, schickt der Bot eine Telegram-Nachricht mit Betrag und Konto. Ob das Geld wirklich angekommen ist, sieht nur die Bank – deshalb ist die automatische Bestätigung standardmässig ausgeschaltet.

## Was der Bot macht

- **Kauf auf der Börse**: Marktorder auf Kraken, Auszahlung der BTC in eine eigene Wallet
- **Angebote auf Peach**: erstellen, ins Escrow einzahlen, eingehende Kaufanfragen annehmen
- **Preis anpassen**: Findet ein Angebot nach 24 h keinen Käufer, senkt der Bot den Aufpreis schrittweise
- **Marktanalyse**: vergleicht die Aufpreise der Konkurrenz und schlägt einen eigenen vor
- **Gewinnrechnung**: jeder Trade wird mit allen Gebühren in einer SQLite-Datenbank erfasst
- **Telegram-Bot**: Status, Kontostände, offene Angebote, Gewinnübersicht und Steuerung per Chat
- **Web-Dashboard** (Flask): Gewinn über Zeit, Trade-Historie, Marktübersicht

## Technisch interessante Teile

- **Bitcoin-Signaturen selbst implementiert** ([core/taproot.py](core/taproot.py)): Die Plattform nutzt Taproot-Escrows. Die Adressberechnung und das Signieren (Schnorr) habe ich nach den offiziellen Spezifikationen (BIP340/341) in Python umgesetzt.
- **Schlüssel aus einer Seed-Phrase ableiten** ([core/hd_keys.py](core/hd_keys.py)): Jedes Angebot bekommt einen eigenen Escrow-Schlüssel, abgeleitet nach BIP32/BIP39 – gleich wie in der offiziellen Peach-App, damit beide dieselben Schlüssel sehen.
- **Nichts blind signieren**: Bevor der Bot BTC ins Escrow schickt, rechnet er die Escrow-Adresse selbst nach. Bevor er eine Freigabe signiert, prüft er, dass sie wirklich an den Käufer geht. Bei einem Single-Sig-Escrow reicht meine Signatur allein, um die Coins zu bewegen – ein Fehler wäre also nicht rückgängig zu machen.
- **Angebotsgrösse in Franken**: Die Plattform begrenzt ein Angebot auf einen CHF-Betrag. Da der BTC-Kurs schwankt, rechnet der Bot die Grenze in jedem Durchlauf neu in Satoshi um, statt mit festen Werten zu arbeiten, die bald veraltet wären ([core/offer_sizing.py](core/offer_sizing.py)).
- **Mehrere Threads**: Die Hauptschleife und der Telegram-Bot laufen parallel und greifen auf dieselben Daten zu; Locks verhindern, dass sie sich in die Quere kommen.
- **Verschlüsselte Zahlungsdaten**: Bankdaten gehen nur PGP-verschlüsselt an den Käufer.

## Projektstruktur

```
├── core/
│   ├── engine.py          # Hauptschleife (alle ~30 s)
│   ├── models.py          # Datenmodelle (Angebot, Vertrag …)
│   ├── hd_keys.py         # Schlüsselableitung aus der Seed-Phrase (BIP32/39)
│   ├── taproot.py         # Taproot-Adressen und Schnorr-Signaturen (BIP340/341)
│   ├── offer_sizing.py    # Angebotsgrössen aus dem CHF-Limit
│   ├── pricing.py         # Aufpreis-Berechnung aus den Konkurrenzangeboten
│   └── trade_logger.py    # Trade-Datenbank (SQLite)
├── exchanges/
│   ├── kraken.py          # Kraken-API
│   └── bitvavo.py         # Bitvavo-API (Alternative)
├── platforms/
│   └── peach.py           # Peach-API
├── notifications/
│   └── telegram_bot.py    # Telegram-Befehle und Meldungen
├── dashboard.py           # Web-Dashboard (Flask)
├── run.py                 # Startpunkt
└── config.example.json    # Konfigurationsvorlage
```

## Technologien

Python, REST-APIs, Bitcoin (Taproot/Schnorr, BIP32/39, PSBT), secp256k1, PGP, SQLite, Flask, Telegram Bot API

## Einrichtung

```bash
pip install requests python-telegram-bot coincurve pgpy flask
cp config.example.json config.json
# config.json mit API-Keys, Seed-Phrase und Zahlungsdaten ausfüllen
python run.py
```

## Hinweis

Der Bot läuft bei mir produktiv. Dieses Repo zeigt den Code als Portfolio-Projekt – es ist keine fertige Lösung zum Nachbauen.

**Veröffentlicht:** Taproot-Implementierung, Schlüsselableitung, Angebotsgrössen, Preisberechnung, die Anbindung an Kraken und Peach, Datenmodell, Datenbank, Telegram-Bot und Dashboard.

**Nicht veröffentlicht:** die eigentliche Handelslogik (wann welches Angebot erstellt, finanziert und angenommen wird), das Bauen der Wallet-Transaktionen und der Umgang mit Zahlungsdaten. In [core/engine.py](core/engine.py) stehen diese Funktionen nur als dokumentierte Hüllen, damit der Ablauf nachvollziehbar bleibt.

Nutzung auf eigenes Risiko.
