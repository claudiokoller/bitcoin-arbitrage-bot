#!/usr/bin/env python3
import json, logging, os, sys, time
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bot")

def load_config(path="config.json"):
    if not os.path.exists(path): log.error(f"{path} not found!"); sys.exit(1)
    with open(path) as f: return json.load(f)

def build_exchanges(config):
    exs = []
    for name, ecfg in config.get("exchanges",{}).items():
        if not ecfg.get("enabled",False): continue
        if name == "kraken":
            from exchanges.kraken import KrakenExchange
            exs.append(KrakenExchange(ecfg))
    return exs

def build_platforms(config):
    ps = []
    for name, pcfg in config.get("platforms",{}).items():
        if not pcfg.get("enabled",False): continue
        if name == "peach":
            from platforms.peach import PeachPlatform
            ps.append(PeachPlatform(pcfg))
    return ps

def main():
    mode = "full"
    if "--telegram" in sys.argv: mode = "telegram"
    elif "--status" in sys.argv: mode = "status"
    print("\n  Trading Bot v3.0\n")
    config = load_config()
    if mode == "telegram":
        tg = config.get("telegram",{})
        if not tg.get("token"): log.error("No telegram token"); sys.exit(1)
        from notifications.telegram_bot import TelegramBot
        TelegramBot(tg["token"], tg.get("chat_id","")).run_polling()
        return
    from core.engine import TradingEngine
    engine = TradingEngine(config)
    for e in build_exchanges(config): engine.add_exchange(e)
    for p in build_platforms(config): engine.add_platform(p)
    if not engine.platforms: log.error("No platforms!"); sys.exit(1)
    if mode == "status":
        for n,p in engine.platforms.items():
            try: p.authenticate()
            except Exception as e: log.error(f"{n}: {e}")
        print(json.dumps(engine.get_status(), indent=2, default=str))
        return
    tg = config.get("telegram",{})
    if tg.get("enabled") and tg.get("token"):
        from notifications.telegram_bot import TelegramBot
        tb = TelegramBot(tg["token"], tg.get("chat_id",""), engine)
        engine.set_notifier(tb.notifier)
        tb.start_in_thread()
        log.info("Telegram running.")
        time.sleep(2)
        if tg.get("chat_id"):
            tb.notifier._send(f"<b>Bot gestartet</b>\n{', '.join(engine.platforms.keys())}\n/status")
    engine.start()

if __name__ == "__main__": main()
