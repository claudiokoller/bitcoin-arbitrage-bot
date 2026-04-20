import asyncio, json, logging, os, threading
from datetime import datetime
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from core.models import OfferStatus
log = logging.getLogger("bot.telegram")

class TelegramNotifier:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self._loop = None
        self._app = None
    def set_app(self, app, loop):
        self._app = app
        self._loop = loop
    def _send(self, text):
        if not self._app or not self._loop: return
        future = asyncio.run_coroutine_threadsafe(self._app.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="HTML"), self._loop)
        future.add_done_callback(lambda f: f.exception() and log.warning(f"Telegram send failed: {f.exception()}"))
    def notify_offer_created(self, oid, prem, rng):
        self._send(f"<b>Offer</b>\n<code>{oid[:16]}</code> | {prem}% | {rng[0]:,}-{rng[1]:,} sats")
    def notify_escrow_funded(self, oid, btc, fiat, currency="CHF"):
        self._send(f"<b>Escrow funded</b>\n<code>{oid[:16]}</code> | {btc:.8f} BTC ({fiat:.2f} {currency})")
    def notify_match(self, oid, mid):
        self._send(f"<b>MATCH!</b>\n<code>{oid[:16]}</code>")
    def notify_dispute(self, cid):
        self._send(f"<b>DISPUTE!</b> <code>{cid[:16]}</code>")
    def notify_error(self, err):
        self._send(f"<b>Error</b>\n<code>{err[:400]}</code>")
    def notify_low_balance(self, fiat, currency="CHF"):
        self._send(f"<b>Low:</b> {fiat:.2f} {currency}")
    def notify_daily_summary(self, s):
        self._send(f"<b>Heute:</b> {s['count']} Trades | {s['total_profit']:.2f} CHF")

class TelegramBot:
    def __init__(self, token, chat_id, engine=None):
        self.token = token
        self.chat_id = str(chat_id)
        self.engine = engine
        self.notifier = TelegramNotifier(token, chat_id)
        self._funding_lock = threading.Lock()
        self._fundings_file = os.path.join(os.path.dirname(__file__), "..", "pending_wallet_fundings.json")
        self._pending_buy_params = {}  # {chat_id: {amount, premium, command}}
    def _auth(self, update):
        return str(update.effective_chat.id) == self.chat_id

    @staticmethod
    def _filter_payment_methods(payment_methods, exclude_methods):
        """Remove excluded methods and drop currencies with no remaining methods."""
        if not exclude_methods:
            return payment_methods
        filtered = {
            cur: [m for m in methods if m not in exclude_methods]
            for cur, methods in payment_methods.items()
        }
        return {cur: methods for cur, methods in filtered.items() if methods}
    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        cid = update.effective_chat.id
        if str(cid) != self.chat_id:
            await update.message.reply_text(f"Chat-ID: <code>{cid}</code>\nTrage sie in config.json ein.", parse_mode="HTML")
            return
        await update.message.reply_text(
            "<b>Trading Bot</b>\n\n"
            "<b>Info</b>\n"
            "/status – Bot-Status\n"
            "/balance – Kraken-Kontostände\n"
            "/wallet – Hot Wallet Balance &amp; UTXOs\n"
            "/offers – Aktive Peach-Angebote\n"
            "/contracts – Aktive Contracts\n"
            "/market – Marktanalyse\n"
            "/profit – Gewinn-Übersicht\n\n"
            "<b>Kaufen + Offer + Escrow (Fiat)</b>\n"
            "/buy_escrow &lt;chf&gt; [premium%]\n"
            "/buy_escrow_norev &lt;chf&gt; [premium%]\n\n"
            "<b>Offer + Kraken-BTC withdrawen</b>\n"
            "/escrow &lt;sats&gt; &lt;premium%&gt;\n"
            "/escrow_norev &lt;sats&gt; &lt;premium%&gt;\n\n"
            "<b>Offer + Hot Wallet funden</b>\n"
            "/fund &lt;sats&gt; &lt;premium%&gt;\n"
            "/fund_norev &lt;sats&gt; &lt;premium%&gt;\n\n"
            "<b>Kraken</b>\n"
            "/buy &lt;betrag&gt; – BTC kaufen\n"
            "/sell &lt;btc&gt; – BTC verkaufen\n\n"
            "<b>Sonstiges</b>\n"
            "/cancel &lt;offer_id&gt; – Offer abbrechen\n"
            "/refunds – Offene Refunds\n"
            "/reload – Config neu laden (kein Restart)\n"
            "/pause /resume – Bot steuern",
            parse_mode="HTML")
    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine: return
        s = self.engine.get_status()
        paused = "PAUSE" if s["paused"] else "AKTIV"
        try:
            from core.engine import SpotPriceProvider
            spot = SpotPriceProvider.get_spot_chf()
        except Exception: spot = 0
        plines = []
        for n, ps in s.get("platforms",{}).items():
            plines.append(f"  {'ON' if ps.get('online') else 'OFF'} {n}")
        elines = []
        for n, es in s.get("exchanges",{}).items():
            if es.get("online"):
                cur = es.get('currency', 'CHF')
                elines.append(f"  {n}: {es.get('fiat_balance',0):,.0f} {cur}")
            else:
                elines.append(f"  {n}: offline")
        pl = "\n".join(plines)
        el = "\n".join(elines)
        uptime = s.get('uptime', '?')
        text = f"<b>Status</b> {paused} ({uptime})\nSpot: {spot:,.0f} CHF\n\n<b>Plattformen</b>\n{pl}\n\n<b>Exchanges</b>\n{el}\n\nOffers: {s['pending_escrows']} pending, {s['funded_escrows']} funded\nHeute: {s['daily_volume_sats']:,} sats"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Refresh",callback_data="refresh_status"), InlineKeyboardButton("Pause" if not s["paused"] else "Resume",callback_data="toggle_pause")]])
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)
    async def cmd_balance(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine: return
        lines = ["<b>Balances</b>\n"]
        btc_shown = False
        loop = asyncio.get_event_loop()
        for n, ex in self.engine.exchanges.items():
            try:
                s = ex.get_status()
                cur = s.get('currency', 'CHF')
                btc = s.get('btc_balance', 0)
                fiat = s.get('fiat_balance', 0)
                if not btc_shown:
                    try:
                        chf_spot = await loop.run_in_executor(None, SpotPriceProvider.get_spot_chf)
                        btc_chf = btc * chf_spot
                        lines.append(f"<b>BTC (Kraken)</b>: {btc:.8f} BTC ({int(btc * 1e8):,} sats) ≈ {btc_chf:,.2f} CHF")
                    except Exception:
                        lines.append(f"<b>BTC (Kraken)</b>: {btc:.8f} BTC ({int(btc * 1e8):,} sats)")
                    btc_shown = True
                lines.append(f"<b>{s.get('name', n)}</b>: {fiat:,.2f} {cur}")
            except Exception as e:
                lines.append(f"<b>{n}</b>: {e}")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    async def cmd_offers(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine: return
        lines = ["<b>Offers</b>\n"]
        all_offers = []
        for n, p in self.engine.platforms.items():
            try:
                offers = p.get_active_offers()
                if not offers:
                    lines.append(f"<b>{n}</b>: keine")
                    continue
                lines.append(f"<b>{n}</b> ({len(offers)}):")
                for o in offers:
                    lines.append(f"  {o.id} | {o.max_sats:,} sats | {o.premium_pct}% | {o.status.value}")
                    all_offers.append(o)
            except Exception as e:
                lines.append(f"<b>{n}</b>: {e}")
        buttons = []
        for o in all_offers:
            buttons.append([InlineKeyboardButton(
                f"Cancel {o.id} ({o.max_sats:,} sats)",
                callback_data=f"cancel_{o.id}"
            )])
        if all_offers:
            buttons.append([InlineKeyboardButton(
                f"Alle canceln ({len(all_offers)})",
                callback_data="cancel_all"
            )])
        markup = InlineKeyboardMarkup(buttons) if buttons else None
        await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=markup)
    async def cmd_pause(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine: return
        self.engine.paused = True
        await update.message.reply_text("<b>Pausiert</b>", parse_mode="HTML")
    async def cmd_resume(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine: return
        self.engine.paused = False
        await update.message.reply_text("<b>Laeuft</b>", parse_mode="HTML")
    async def cmd_profit(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine: return
        d = self.engine.trade_logger.get_daily_summary()
        m = self.engine.trade_logger.get_period_summary(30)
        b = self.engine.trade_logger.get_platform_breakdown(30)
        bl = "".join(f"  {x['platform']}: {x['count']} trades, {x['profit']:.2f} CHF\n" for x in b)
        text = f"<b>Profit</b>\nHeute: {d['count']} | {d['total_profit']:.2f} CHF\n30d: {m['count']} | {m['total_profit']:.2f} CHF\n\n<b>Plattformen</b>\n{bl}"
        await update.message.reply_text(text, parse_mode="HTML")
    # Store last market snapshot for trend comparison
    _last_market_snapshot = None

    async def cmd_market(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine: return
        peach = self.engine.platforms.get("peach")
        if not peach:
            await update.message.reply_text("Peach nicht verfügbar."); return
        await update.message.reply_text("Analysiere Peach Markt…")
        loop = asyncio.get_event_loop()
        try:
            from collections import Counter
            pconfig = self.engine.config.get("platforms", {}).get("peach", {})
            my_methods = pconfig.get("payment_methods", {"CHF": ["twint", "revolut", "wise"], "EUR": ["sepa", "instantSepa", "revolut", "wise"]})
            my_currencies = list(my_methods.keys())
            my_method_set = set()
            for methods in my_methods.values():
                my_method_set.update(methods)
            sells_raw = await loop.run_in_executor(None, lambda: peach.scan_market(currencies=my_currencies, payment_methods=my_methods))
            buys_raw  = await loop.run_in_executor(None, lambda: peach.scan_buy_offers(currencies=my_currencies, payment_methods=my_methods))

            # Get own offer IDs to exclude from competitor analysis
            own_ids = set()
            try:
                own_offers = await loop.run_in_executor(None, peach.get_active_offers)
                own_ids = {str(o.id) for o in own_offers}
            except Exception:
                pass

            # EUR→CHF conversion for offers without priceInCHF
            eur_chf_rate = None
            try:
                from core.engine import SpotPriceProvider
                chf = await loop.run_in_executor(None, SpotPriceProvider.get_spot_chf)
                eur = await loop.run_in_executor(None, SpotPriceProvider.get_spot_eur)
                if eur and chf:
                    eur_chf_rate = chf / eur
            except Exception:
                pass

            # Parse sell offers (exclude own)
            sells_all = []
            method_counts_sell = Counter()
            for o in sells_raw:
                oid = str(o.get("id", ""))
                if oid in own_ids:
                    continue
                prem = float(o.get("premium", 0))
                if not (-5 <= prem <= 25): continue
                chf = float(o.get("priceInCHF", 0))
                if chf <= 0 and eur_chf_rate:
                    eur_val = float(o.get("priceInEUR", o.get("price", 0)))
                    if eur_val > 0:
                        chf = eur_val * eur_chf_rate
                if chf <= 0: continue
                methods = o.get("meansOfPayment", {})
                for cur_methods in methods.values():
                    if isinstance(cur_methods, list):
                        for m in cur_methods:
                            method_counts_sell[m] += 1
                sells_all.append({"premium": prem, "chf": chf})

            # Parse buy offers (exclude own)
            buys_all = []
            method_counts_buy = Counter()
            for o in buys_raw:
                oid = str(o.get("id", ""))
                if oid in own_ids:
                    continue
                chf = float(o.get("priceInCHF", 0))
                if chf <= 0 and eur_chf_rate:
                    eur_val = float(o.get("priceInEUR", o.get("price", 0)))
                    if eur_val > 0:
                        chf = eur_val * eur_chf_rate
                if chf <= 0: continue
                methods = o.get("meansOfPayment", {})
                for cur_methods in methods.values():
                    if isinstance(cur_methods, list):
                        for m in cur_methods:
                            method_counts_buy[m] += 1
                buys_all.append({"chf": chf})

            if not sells_all:
                await update.message.reply_text("Keine Verkaufs-Angebote gefunden."); return

            # Focus range: 100-600 CHF
            MY_LO, MY_HI = 100, 600
            sells_focus = [s for s in sells_all if MY_LO <= s["chf"] <= MY_HI]
            buys_focus = [b for b in buys_all if MY_LO <= b["chf"] <= MY_HI]

            premiums_all = sorted(s["premium"] for s in sells_all)
            premiums_focus = sorted(s["premium"] for s in sells_focus)
            n_s = len(sells_all); n_b = len(buys_all)
            n_sf = len(sells_focus); n_bf = len(buys_focus)

            # Premium distribution in focus range
            prem_buckets = Counter(int(p) for p in premiums_focus) if premiums_focus else Counter()

            # Amount buckets
            ranges = [("&lt;100", 0, 100), ("100–200", 100, 200), ("200–300", 200, 300), ("300–400", 300, 400), ("400–600", 400, 600), ("600+", 600, 9999)]
            def in_range(chf, lo, hi): return lo <= chf < hi
            s_counts = {lbl: sum(1 for s in sells_all if in_range(s["chf"], lo, hi)) for lbl, lo, hi in ranges}
            b_counts = {lbl: sum(1 for b in buys_all if in_range(b["chf"], lo, hi)) for lbl, lo, hi in ranges}

            # Premium recommendation
            pricer_cfg = self.engine.pricer.configs.get("peach")
            floor = pricer_cfg.floor_pct if pricer_cfg else 3.0
            if premiums_focus:
                lowest_3 = premiums_focus[:3] if len(premiums_focus) >= 3 else premiums_focus
                avg_lowest = sum(lowest_3) / len(lowest_3)
                best_prem = round((avg_lowest - 0.5) * 2) / 2
                gap_reason = f"Unterbiete tiefste {len(lowest_3)} in {MY_LO}–{MY_HI} CHF ({avg_lowest:+.1f}%)"
            else:
                lowest_3 = premiums_all[:3] if len(premiums_all) >= 3 else premiums_all
                avg_lowest = sum(lowest_3) / len(lowest_3)
                best_prem = round((avg_lowest - 0.5) * 2) / 2
                gap_reason = f"Keine Konkurrenz in {MY_LO}–{MY_HI} CHF, unterbiete Markt ({avg_lowest:+.1f}%)"
            best_prem = max(floor, min(best_prem, 10.0))

            # Recommended amount
            focus_ranges = [("100–200", 100, 200), ("200–300", 200, 300), ("300–400", 300, 400), ("400–600", 400, 600)]
            best_sub = focus_ranges[0]; best_ratio = -1
            for lbl, lo, hi in focus_ranges:
                sc = s_counts.get(lbl, 0); bc = b_counts.get(lbl, 0)
                score = bc / (sc + 1)
                if score > best_ratio:
                    best_ratio = score; best_sub = (lbl, lo, hi)
            rec_chf = round((best_sub[1] + best_sub[2]) / 2 / 50) * 50

            # Spread: lowest seller premium vs market
            spread_str = ""
            if premiums_focus:
                lowest_sell = min(premiums_focus)
                spread_str = f"Tiefste Prämie: {lowest_sell:+.1f}%"

            # Trend: compare with last scan
            trend_str = ""
            current_snapshot = {"n_s": n_s, "n_b": n_b, "n_sf": n_sf, "n_bf": n_bf,
                               "avg_prem": avg_lowest if premiums_focus or premiums_all else 0}
            prev = TelegramBot._last_market_snapshot
            if prev:
                ds = n_s - prev["n_s"]; db = n_b - prev["n_b"]
                dp = current_snapshot["avg_prem"] - prev["avg_prem"]
                parts = []
                if ds != 0: parts.append(f"Seller {ds:+d}")
                if db != 0: parts.append(f"Käufer {db:+d}")
                if abs(dp) >= 0.1: parts.append(f"Prämie {dp:+.1f}%")
                if parts:
                    trend_str = " | ".join(parts)
            TelegramBot._last_market_snapshot = current_snapshot

            # Payment method breakdown (only methods I offer)
            method_labels = {"twint": "Twint", "revolut": "Revolut", "wise": "Wise",
                            "sepa": "SEPA", "instantSepa": "Instant SEPA"}
            mlines = []
            all_methods = sorted(my_method_set,
                                key=lambda m: method_counts_buy.get(m, 0), reverse=True)
            for m in all_methods:
                label = method_labels.get(m, m)
                sc = method_counts_sell.get(m, 0)
                bc = method_counts_buy.get(m, 0)
                ratio = f"{bc/(sc+1):.1f}x"
                mlines.append(f"  {label}: S:{sc} B:{bc} ({ratio})")

            # Format
            def bar(n, mx): f = round(n / max(mx, 1) * 7); return "█" * f + "░" * (7 - f)

            # Premium bars per amount bucket (same as Nachfrage)
            plines = []
            for lbl, lo, hi in ranges:
                if lo < MY_LO or hi > MY_HI + 1:
                    continue
                bucket_prems = sorted(s["premium"] for s in sells_all if in_range(s["chf"], lo, hi))
                if bucket_prems:
                    avg_p = sum(bucket_prems) / len(bucket_prems)
                    low_p = min(bucket_prems)
                    plines.append(f"  {lbl} CHF: {low_p:+.1f}% – {max(bucket_prems):+.1f}% (Ø {avg_p:+.1f}%, {len(bucket_prems)}x)")
                else:
                    plines.append(f"  {lbl} CHF: (keine)")
            if not plines:
                plines = ["  (keine Angebote in diesem Bereich)"]

            clines = []
            for lbl, lo, hi in ranges:
                sc = s_counts[lbl]; bc = b_counts[lbl]
                ratio_str = f"{bc/(sc+1):.1f}x" if sc+bc > 0 else "–"
                focus = " ←" if MY_LO <= lo and hi <= MY_HI + 1 else ""
                clines.append(f"  {lbl} CHF: S:{sc} B:{bc} ({ratio_str}){focus}")

            own_str = f"  (eigene {len(own_ids)} Offers ausgeblendet)\n" if own_ids else ""

            text = (
                f"<b>Markt</b>  {n_s} Seller | {n_b} Käufer\n{own_str}"
            )
            if trend_str:
                text += f"<b>Trend:</b> {trend_str}\n"
            text += "\n"

            if premiums_focus:
                text += (
                    f"<b>Fokus {MY_LO}–{MY_HI} CHF:</b> {n_sf} Seller | {n_bf} Käufer ({n_bf/(n_sf+1):.1f}x)\n"
                    f"Prämien: {min(premiums_focus):+.1f}% – {max(premiums_focus):+.1f}%\n"
                )
                if spread_str:
                    text += f"{spread_str}\n"
                text += "\n"
            else:
                text += f"<b>Fokus {MY_LO}–{MY_HI} CHF:</b> keine Seller | {n_bf} Käufer\n\n"

            text += (
                f"<b>Prämien ({MY_LO}–{MY_HI} CHF):</b>\n" + "\n".join(plines) + "\n\n"
                f"<b>Nachfrage (S=Seller B=Käufer):</b>\n" + "\n".join(clines) + "\n\n"
            )
            if mlines:
                text += f"<b>Zahlungsmethoden:</b>\n" + "\n".join(mlines) + "\n\n"
            text += (
                f"<b>Empfehlung:</b>\n"
                f"  Prämie: <b>{best_prem:+.1f}%</b> — {gap_reason}\n"
                f"  Betrag: <b>{rec_chf} CHF</b> — Nachfrage {best_ratio:.1f}x in {best_sub[0]} CHF\n\n"
                f"→ <code>/buy_escrow {rec_chf} {best_prem:.1f}</code>"
            )
            await update.message.reply_text(text, parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"Fehler: {e}")
            log.exception(f"cmd_market: {e}")
    async def handle_callback(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if str(q.message.chat.id) != self.chat_id: return
        if q.data == "toggle_pause":
            self.engine.paused = not self.engine.paused
            await q.edit_message_text(f"Bot: {'pause' if self.engine.paused else 'laeuft'}", parse_mode="HTML")
        elif q.data == "refresh_status":
            update.message = q.message
            await self.cmd_status(update, ctx)
        elif q.data == "rescan_market":
            update.message = q.message
            await self.cmd_market(update, ctx)
        elif q.data.startswith("buyescrow_"):
            await self._handle_currency_callback(q, "buy_escrow")
        elif q.data.startswith("escrow_") and q.data[7:] in ("chf", "eur", "usd"):
            await self._handle_currency_callback(q, "escrow")
        elif q.data.startswith("sell_") and q.data[5:] in ("chf", "eur", "usd"):
            await self._handle_currency_callback(q, "sell")
        elif q.data.startswith("buy_") and q.data[4:] in ("chf", "eur", "usd"):
            await self._handle_currency_callback(q, "buy")
        elif q.data.startswith("cancel_"):
            await self._handle_cancel_callback(q)
        elif q.data.startswith("confirm_cancel_"):
            await self._handle_confirm_cancel(q)
    async def _handle_cancel_callback(self, q):
        """Show confirmation before cancelling"""
        data = q.data
        if data == "cancel_all":
            buttons = [[InlineKeyboardButton(
                "Ja, ALLE canceln", callback_data="confirm_cancel_all"
            )]]
            await q.edit_message_text(
                "Wirklich <b>alle</b> Offers canceln?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            offer_id = data.replace("cancel_", "")
            buttons = [[InlineKeyboardButton(
                f"Ja, {offer_id} canceln", callback_data=f"confirm_cancel_{offer_id}"
            )]]
            await q.edit_message_text(
                f"Offer <code>{offer_id}</code> wirklich canceln?",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(buttons)
            )

    async def _handle_confirm_cancel(self, q):
        """Execute the cancel after confirmation"""
        data = q.data.replace("confirm_cancel_", "")
        peach = self.engine.platforms.get("peach")
        if not peach:
            await q.edit_message_text("Peach nicht verfügbar.")
            return
        loop = asyncio.get_event_loop()
        if data == "all":
            try:
                offers = await loop.run_in_executor(None, peach.get_active_offers)
                cancelled = []
                for o in offers:
                    try:
                        await loop.run_in_executor(None, self.engine.register_refund, o.id)
                        await loop.run_in_executor(None, peach.cancel_offer, o.id)
                        with self.engine._escrow_lock:
                            self.engine.pending_escrows.pop(o.id, None)
                        cancelled.append(o.id)
                    except Exception as e:
                        log.warning(f"Cancel {o.id}: {e}")
                await q.edit_message_text(
                    f"<b>{len(cancelled)} Offers abgebrochen</b>\n" +
                    "\n".join(f"• {oid}" for oid in cancelled),
                    parse_mode="HTML"
                )
            except Exception as e:
                await q.edit_message_text(f"Fehler: {e}")
        else:
            offer_id = data
            try:
                await loop.run_in_executor(None, self.engine.register_refund, offer_id)
                await loop.run_in_executor(None, peach.cancel_offer, offer_id)
                with self.engine._escrow_lock:
                    self.engine.pending_escrows.pop(offer_id, None)
                await q.edit_message_text(
                    f"<b>Offer abgebrochen</b>\n<code>{offer_id}</code>",
                    parse_mode="HTML"
                )
            except Exception as e:
                await q.edit_message_text(f"Fehler: {e}")

    async def _handle_currency_callback(self, q, command):
        """Handle currency selection for buy/buy_escrow commands."""
        chat_id = str(q.message.chat.id)
        params = self._pending_buy_params.pop(chat_id, None)
        if not params:
            await q.edit_message_text("Abgelaufen. Bitte Befehl erneut eingeben.")
            return
        # Extract currency from callback data
        if command == "buy_escrow":
            currency = q.data.replace("buyescrow_", "").upper()
        elif command == "sell":
            currency = q.data.replace("sell_", "").upper()
        elif command == "escrow":
            currency = q.data.replace("escrow_", "").upper()
        else:
            currency = q.data.replace("buy_", "").upper()
        if currency == "BTC":
            exchange = self.engine.get_best_exchange()
        else:
            exchange = self.engine.get_exchange_by_currency(currency)
        if not exchange:
            await q.edit_message_text(f"Kein Exchange für {currency} konfiguriert.")
            return
        await q.edit_message_text(f"{currency} ausgewählt...")
        # Use bot.send_message as reply function (callback messages can't be used as update.message)
        bot = q.get_bot()
        async def _send(text, **kw):
            await bot.send_message(chat_id=chat_id, text=text, **kw)
        if command == "buy_escrow" and currency == "BTC":
            # BTC flow: amount is in sats, no Kraken buy — just withdraw existing BTC
            await self._execute_escrow(exchange, int(params["amount"]), params.get("premium"), send_fn=_send, exclude_methods=params.get("exclude_methods"))
        elif command == "buy_escrow":
            await self._execute_buy_escrow(exchange, params["amount"], params.get("premium"), send_fn=_send, exclude_methods=params.get("exclude_methods"))
        elif command == "buy":
            await self._execute_buy(exchange, params["amount"], send_fn=_send)
        elif command == "sell":
            await self._execute_sell(exchange, params.get("amount_btc"), send_fn=_send)
        elif command == "escrow":
            await self._execute_escrow(exchange, params["requested_sats"], params["premium"], send_fn=_send, exclude_methods=params.get("exclude_methods"))

    def _get_available_currencies(self):
        """Get list of fiat currencies from configured exchanges, plus BTC if available."""
        currencies = []
        for ex in self.engine.exchanges.values():
            if hasattr(ex, 'get_fiat_currency'):
                c = ex.get_fiat_currency()
                if c not in currencies:
                    currencies.append(c)
        currencies.append("BTC")
        return currencies

    async def _parse_buy_escrow_args(self, update, ctx):
        """Parse und validiere buy_escrow Argumente. Gibt (amount, premium) oder None zurück.
        amount = Fiat-Betrag (CHF/EUR/USD) ODER Sats-Betrag (wenn BTC gewählt wird).
        """
        args = ctx.args
        if not args:
            await update.message.reply_text(
                "Verwendung: /buy_escrow &lt;betrag&gt; [premium%]\n"
                "Fiat: /buy_escrow 200 6.5  (CHF/EUR/USD)\n"
                "BTC:  /buy_escrow 400000 6.5  (Sats)",
                parse_mode="HTML"); return None
        try:
            amount_fiat = float(args[0])
        except ValueError:
            await update.message.reply_text("Ungültiger Betrag."); return None
        if amount_fiat <= 0:
            await update.message.reply_text("Betrag muss grösser als 0 sein."); return None
        manual_premium = None
        if len(args) >= 2:
            try:
                manual_premium = float(args[1])
            except ValueError:
                await update.message.reply_text("Ungültiges Premium (z.B. 6.5)."); return None
        return amount_fiat, manual_premium

    async def _buy_escrow_with_methods(self, update, ctx, exclude_methods=None):
        """Gemeinsame Logik für buy_escrow mit optionalem Methoden-Ausschluss."""
        if not self._auth(update) or not self.engine: return
        parsed = await self._parse_buy_escrow_args(update, ctx)
        if not parsed: return
        amount_fiat, manual_premium = parsed

        # Check if multiple currencies available — ask user to pick
        currencies = self._get_available_currencies()
        if len(currencies) > 1:
            params = {"command": "buy_escrow", "amount": amount_fiat, "premium": manual_premium}
            if exclude_methods:
                params["exclude_methods"] = exclude_methods
            self._pending_buy_params[str(update.effective_chat.id)] = params
            def _btn_label(c):
                if c == "BTC":
                    return f"BTC ({int(amount_fiat):,} sats)"
                return c
            buttons = [[InlineKeyboardButton(_btn_label(c), callback_data=f"buyescrow_{c.lower()}") for c in currencies]]
            await update.message.reply_text(
                f"<b>{amount_fiat:.0f} — Währung / Quelle wählen:</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
            return

        # Single exchange — use it directly
        exchange = self.engine.get_best_exchange()
        if not exchange:
            await update.message.reply_text("Kein Exchange verfügbar."); return
        await self._execute_buy_escrow(exchange, amount_fiat, manual_premium, send_fn=update.message.reply_text, exclude_methods=exclude_methods)

    def trigger_auto_buy_escrow(self, exchange, amount_fiat, exclude_methods=None):
        """Called by engine to auto-trigger buy_escrow_norev. Runs in background thread."""
        threading.Thread(
            target=self._run_auto_buy_escrow,
            args=(exchange, amount_fiat, exclude_methods),
            daemon=True, name=f"auto-buy-{exchange.name[:10]}"
        ).start()

    def trigger_auto_fund_wallet(self, amount_sats, exclude_methods=None):
        """Called by engine when unallocated confirmed BTC sits on the hot wallet."""
        threading.Thread(
            target=self._run_auto_fund_wallet,
            args=(amount_sats, exclude_methods),
            daemon=True, name="auto-fund-wallet"
        ).start()

    def _run_auto_fund_wallet(self, amount_sats, exclude_methods=None):
        """Create a Peach offer and fund it directly from the hot wallet (no Kraken buy)."""
        peach = self.engine.platforms.get("peach")
        if not peach:
            return
        pconfig = self.engine.config.get("platforms", {}).get("peach", {})
        hot_wallet_addr = pconfig.get("refund_address", "")
        if not hot_wallet_addr:
            return

        label = f" (ohne {', '.join(exclude_methods)})" if exclude_methods else ""
        log.info(f"auto_fund_wallet: creating offer for {amount_sats:,} sats{label}")
        try:
            min_sats = pconfig.get("min_amount_sats", 10000)
            max_sats = max(min(amount_sats, pconfig.get("max_amount_sats", 560000)), min_sats)
            payment_methods = self._filter_payment_methods(
                pconfig.get("payment_methods", {"EUR": ["sepa", "revolut"]}), exclude_methods)
            auto_cfg = self.engine.config.get("auto_buy_escrow", {})
            premium = auto_cfg.get("premium", 5.5)

            offer = peach.create_sell_offer(min_sats=min_sats, max_sats=max_sats,
                                            premium_pct=premium, payment_methods=payment_methods)
            escrow_addr = offer.escrow_address
            if not escrow_addr:
                try:
                    info = peach.create_escrow(offer.id)
                    escrow_addr = info.get("address", "")
                except Exception as e:
                    log.warning(f"auto_fund_wallet create_escrow: {e}")
            self.engine.add_pending_escrow(offer.id, escrow_addr, max_sats, premium)
            log.info(f"auto_fund_wallet: offer {offer.id} escrow={escrow_addr} amount={max_sats}")

            self.notifier._send(
                f"<b>🏦 Auto Fund from Wallet{label}</b>\n"
                f"Offer <code>{offer.id[:16]}</code> @ {premium:.1f}%\n"
                f"{min_sats:,}–{max_sats:,} sats\n"
                f"ℹ️ BTC bereits auf Hot Wallet — kein Kraken-Kauf")

            if not escrow_addr:
                log.error(f"auto_fund_wallet: no escrow_addr for {offer.id}")
                return
            self._save_pending_funding(offer.id, escrow_addr, max_sats, hot_wallet_addr)
            threading.Thread(
                target=self._poll_and_fund_escrow,
                args=(offer.id, escrow_addr, max_sats, hot_wallet_addr),
                daemon=True, name=f"fund-{offer.id[:8]}").start()
        except Exception as e:
            log.exception(f"auto_fund_wallet: {e}")
            self.notifier._send(f"❌ <b>Auto Fund Wallet Fehler</b>\n{str(e)[:300]}")

    def _run_auto_buy_escrow(self, exchange, amount_fiat, exclude_methods=None):
        """Synchronous buy-escrow flow for auto-trigger. Mirrors _execute_buy_escrow."""
        currency = exchange.get_fiat_currency()
        peach = self.engine.platforms.get("peach")
        if not peach:
            return
        pconfig = self.engine.config.get("platforms", {}).get("peach", {})
        hot_wallet_addr = pconfig.get("refund_address", "")
        if not hot_wallet_addr:
            return

        def _notify(text):
            self.notifier._send(text)

        label = f" (ohne {', '.join(exclude_methods)})" if exclude_methods else ""
        log.info(f"auto_buy_escrow: {amount_fiat:.0f} {currency}{label}")
        try:
            spot = exchange.get_spot_price()
            # Deduct Kraken withdrawal fee so the arriving UTXO covers the escrow amount
            # without needing to combine multiple UTXOs (avoids UTXO competition bug).
            withdraw_fee_sats = 0
            if hasattr(exchange, "get_withdrawal_fee_sats"):
                try:
                    withdraw_fee_sats = exchange.get_withdrawal_fee_sats()
                except Exception:
                    withdraw_fee_sats = 15_000
            gross_sats = int((amount_fiat / spot) * 1e8)
            amount_sats = int(gross_sats * 0.99) - withdraw_fee_sats
            min_sats = pconfig.get("min_amount_sats", 10000)
            max_sats = max(min(amount_sats, pconfig.get("max_amount_sats", 560000)), min_sats)
            payment_methods = self._filter_payment_methods(
                pconfig.get("payment_methods", {"EUR": ["sepa", "revolut"]}), exclude_methods)
            auto_cfg = self.engine.config.get("auto_buy_escrow", {})
            premium = auto_cfg.get("premium", 5.5)

            offer = peach.create_sell_offer(min_sats=min_sats, max_sats=max_sats,
                                            premium_pct=premium, payment_methods=payment_methods)
            escrow_addr = offer.escrow_address
            if not escrow_addr:
                try:
                    info = peach.create_escrow(offer.id)
                    escrow_addr = info.get("address", "")
                except Exception as e:
                    log.warning(f"auto_buy_escrow create_escrow: {e}")
            self.engine.add_pending_escrow(offer.id, escrow_addr, max_sats, premium)
            log.info(f"auto_buy_escrow: offer {offer.id} escrow={escrow_addr} amount={max_sats}")

            # Check hot wallet — skip Kraken buy if already sufficient
            hot_wallet_sufficient = False
            hw_confirmed = 0
            try:
                from fund_from_wallet import get_utxos
                hw_utxos = get_utxos(hot_wallet_addr)
                hw_confirmed = sum(u["value"] for u in hw_utxos if u.get("status", {}).get("confirmed", False))
                if hw_confirmed >= max_sats + 2000:
                    hot_wallet_sufficient = True
                    log.info(f"auto_buy_escrow: hot wallet {hw_confirmed:,} sats — skipping Kraken buy")
            except Exception as hw_err:
                log.warning(f"auto_buy_escrow: hot wallet check failed: {hw_err}")

            if hot_wallet_sufficient:
                _notify(
                    f"<b>🤖 Auto Buy-Escrow{label}</b>\n"
                    f"Offer <code>{offer.id[:16]}</code> @ {premium:.1f}%\n"
                    f"{min_sats:,}–{max_sats:,} sats\n"
                    f"ℹ️ Hot Wallet hat genug ({hw_confirmed:,} sats) — Kraken übersprungen")
            else:
                buy = exchange.buy_btc_market(amount_fiat)
                spot_at_buy = buy.effective_price or (buy.fiat_spent / buy.btc_amount if buy.btc_amount else 0)
                with self.engine._escrow_lock:
                    if offer.id in self.engine.pending_escrows:
                        self.engine.pending_escrows[offer.id]["buy_data"] = {
                            "fiat_spent": buy.fiat_spent, "exchange_fee": buy.fee_fiat,
                            "spot_at_buy": spot_at_buy, "btc_amount": buy.btc_amount,
                            "buy_currency": currency}
                actual_balance = exchange.get_btc_balance()
                withdraw_amount = min(buy.btc_amount, actual_balance)
                withdrawal = exchange.withdraw_btc("", withdraw_amount)
                log.info(f"auto_buy_escrow: withdrawal {withdrawal.withdrawal_id} ({buy.btc_amount:.8f} BTC)")
                _notify(
                    f"<b>🤖 Auto Buy-Escrow{label}</b>\n"
                    f"Offer <code>{offer.id[:16]}</code> @ {premium:.1f}%\n"
                    f"{min_sats:,}–{max_sats:,} sats\n"
                    f"✅ {buy.btc_amount:.8f} BTC für {buy.fiat_spent:.2f} {currency}\n"
                    f"Withdrawal: <code>{withdrawal.withdrawal_id}</code>")

            if not escrow_addr:
                log.error(f"auto_buy_escrow: no escrow_addr for {offer.id}")
                return
            self._save_pending_funding(offer.id, escrow_addr, max_sats, hot_wallet_addr)
            threading.Thread(
                target=self._poll_and_fund_escrow,
                args=(offer.id, escrow_addr, max_sats, hot_wallet_addr),
                daemon=True, name=f"fund-{offer.id[:8]}").start()
        except Exception as e:
            log.exception(f"auto_buy_escrow: {e}")
            self.notifier._send(f"❌ <b>Auto Buy-Escrow Fehler</b> ({currency})\n{str(e)[:300]}")

    async def cmd_fund(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Peach-Offer erstellen + direkt von Hot Wallet funden (alle Methoden)"""
        await self._fund_with_methods(update, ctx)

    async def cmd_fund_norev(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Wie /fund, aber ohne Revolut"""
        await self._fund_with_methods(update, ctx, exclude_methods=["revolut"])

    async def _fund_with_methods(self, update, ctx, exclude_methods=None):
        if not self._auth(update) or not self.engine: return
        args = ctx.args
        cmd = "/fund" if not exclude_methods else "/fund_norev"
        if not args or len(args) < 2:
            await update.message.reply_text(
                f"Verwendung: {cmd} &lt;sats&gt; &lt;premium%&gt;\n"
                f"Beispiel: {cmd} 500000 5.5\n\n"
                "Nutzt vorhandenes BTC auf Hot Wallet (kein Kraken).",
                parse_mode="HTML"); return
        try:
            requested_sats = int(args[0])
        except ValueError:
            await update.message.reply_text("Ungültige Sats-Menge."); return
        if requested_sats < 10000:
            await update.message.reply_text("Minimum 10'000 sats."); return
        try:
            premium = float(args[1])
        except ValueError:
            await update.message.reply_text("Ungültiges Premium (z.B. 5.5)."); return

        peach = self.engine.platforms.get("peach")
        if not peach:
            await update.message.reply_text("Peach nicht verfügbar."); return
        pconfig = self.engine.config.get("platforms", {}).get("peach", {})
        hot_wallet_addr = pconfig.get("refund_address", "")
        if not hot_wallet_addr:
            await update.message.reply_text("Kein refund_address in config.json."); return

        # Check hot wallet balance
        loop = asyncio.get_event_loop()
        try:
            from fund_from_wallet import get_utxos
            utxos = await loop.run_in_executor(None, get_utxos, hot_wallet_addr)
            confirmed = [u for u in utxos if u.get("status", {}).get("confirmed", False)]
            total_confirmed = sum(u["value"] for u in confirmed)
            if total_confirmed < requested_sats + 2000:  # sats + fee buffer
                await update.message.reply_text(
                    f"Nicht genug confirmed sats auf Hot Wallet.\n"
                    f"Verfügbar: {total_confirmed:,} sats\nBenötigt: ~{requested_sats + 2000:,} sats")
                return
        except Exception as e:
            await update.message.reply_text(f"Hot Wallet Check fehlgeschlagen: {e}"); return

        async def _tg(text, **kw):
            try:
                await update.message.reply_text(text, **kw)
            except Exception as tg_err:
                log.warning(f"TG send failed: {tg_err}")

        label = f" (ohne {', '.join(exclude_methods)})" if exclude_methods else ""
        try:
            min_sats = pconfig.get("min_amount_sats", 10000)
            max_sats = max(min(requested_sats, pconfig.get("max_amount_sats", 560000)), min_sats)

            await _tg(
                f"<b>Fund-Flow{label}</b>\n"
                f"{requested_sats:,} sats von Hot Wallet\n\n"
                f"Schritt 1/2: Offer erstellen…",
                parse_mode="HTML")

            payment_methods = self._filter_payment_methods(
                pconfig.get("payment_methods", {"EUR": ["sepa", "revolut"]}), exclude_methods)

            offer = await loop.run_in_executor(None, lambda: peach.create_sell_offer(
                min_sats=min_sats, max_sats=max_sats,
                premium_pct=premium, payment_methods=payment_methods))
            escrow_addr = offer.escrow_address
            if not escrow_addr:
                try:
                    info = await loop.run_in_executor(None, lambda: peach.create_escrow(offer.id))
                    escrow_addr = info.get("address", "")
                except Exception as e:
                    log.warning(f"create_escrow: {e}")

            self.engine.add_pending_escrow(offer.id, escrow_addr, max_sats, premium)
            log.info(f"fund: offer {offer.id} escrow={escrow_addr} amount={max_sats}")

            await _tg(
                f"✅ Offer: <code>{offer.id[:16]}</code> @ {premium}%\n"
                f"{min_sats:,}–{max_sats:,} sats\n\n"
                f"Schritt 2/2: Hot Wallet → Escrow…",
                parse_mode="HTML")

            self._save_pending_funding(offer.id, escrow_addr, max_sats, hot_wallet_addr)
            threading.Thread(
                target=self._poll_and_fund_escrow,
                args=(offer.id, escrow_addr, max_sats, hot_wallet_addr),
                daemon=True, name=f"fund-{offer.id[:8]}").start()

        except Exception as e:
            await _tg(f"❌ Fehler: {str(e)[:400]}")
            log.exception(f"fund: {e}")

    async def cmd_buy_escrow(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Kaufe auf Kraken → Hot Wallet → erstelle Peach-Offer → finanziere Escrow automatisch"""
        await self._buy_escrow_with_methods(update, ctx)

    async def cmd_escrow(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Peach-Offer erstellen + vorhandenes BTC von Kraken withdrawen. Kein Neukauf."""
        if not self._auth(update) or not self.engine: return
        await self._escrow_with_methods(update, ctx)

    async def cmd_escrow_norev(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Wie /escrow, aber ohne Revolut als Zahlungsmethode"""
        await self._escrow_with_methods(update, ctx, exclude_methods=["revolut"])

    async def _escrow_with_methods(self, update, ctx, exclude_methods=None):
        if not self._auth(update) or not self.engine: return
        args = ctx.args
        if not args or len(args) < 2:
            cmd = "/escrow" if not exclude_methods else "/escrow_norev"
            await update.message.reply_text(
                f"Verwendung: {cmd} &lt;sats&gt; &lt;premium%&gt;\n"
                f"Beispiel: {cmd} 500000 5.5\n\n"
                "Nutzt vorhandenes BTC auf Kraken (kein Neukauf).",
                parse_mode="HTML"); return
        try:
            requested_sats = int(args[0])
        except ValueError:
            await update.message.reply_text("Ungültige Sats-Menge (z.B. 500000)."); return
        if requested_sats < 10000:
            await update.message.reply_text("Minimum 10'000 sats."); return
        try:
            premium = float(args[1])
        except ValueError:
            await update.message.reply_text("Ungültiges Premium (z.B. 5.5)."); return

        # Check which exchanges have BTC
        exchanges_with_btc = []
        for ex in self.engine.exchanges.values():
            try:
                btc = ex.get_btc_balance()
                if btc > 0.00001:
                    exchanges_with_btc.append((ex, btc))
            except Exception:
                pass
        if not exchanges_with_btc:
            await update.message.reply_text("Kein BTC auf Kraken vorhanden."); return

        # Check if enough BTC for requested sats
        requested_btc = requested_sats / 1e8
        exchanges_with_btc = [(ex, btc) for ex, btc in exchanges_with_btc if btc >= requested_btc * 0.99]
        if not exchanges_with_btc:
            await update.message.reply_text(f"Nicht genug BTC auf Kraken für {requested_sats:,} sats."); return

        if len(exchanges_with_btc) > 1:
            self._pending_buy_params[str(update.effective_chat.id)] = {
                "command": "escrow", "premium": premium, "exclude_methods": exclude_methods,
                "requested_sats": requested_sats
            }
            buttons = [[InlineKeyboardButton(
                f"{ex.get_fiat_currency()} ({btc:.8f} BTC)",
                callback_data=f"escrow_{ex.get_fiat_currency().lower()}"
            ) for ex, btc in exchanges_with_btc]]
            await update.message.reply_text(
                "<b>Escrow — Exchange wählen:</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
            return

        exchange, btc_balance = exchanges_with_btc[0]
        await self._execute_escrow(exchange, requested_sats, premium, send_fn=update.message.reply_text, exclude_methods=exclude_methods)

    async def _execute_escrow(self, exchange, requested_sats, premium, send_fn, exclude_methods=None):
        """Create Peach offer and withdraw existing BTC from Kraken to fund it."""
        currency = exchange.get_fiat_currency()
        peach = self.engine.platforms.get("peach")
        if not peach:
            await send_fn("Peach nicht verfügbar."); return
        pconfig = self.engine.config.get("platforms", {}).get("peach", {})
        hot_wallet_addr = pconfig.get("refund_address", "")
        if not hot_wallet_addr:
            await send_fn("Kein refund_address in config.json."); return

        async def _tg(text, **kw):
            try:
                await send_fn(text, **kw)
            except Exception as tg_err:
                log.warning(f"TG send failed: {tg_err}")

        label = ""
        if exclude_methods:
            label = f" (ohne {', '.join(exclude_methods)})"

        loop = asyncio.get_event_loop()
        try:
            spot = await loop.run_in_executor(None, exchange.get_spot_price)
            amount_fiat = (requested_sats / 1e8) * spot
            min_sats = pconfig.get("min_amount_sats", 10000)
            max_sats = max(min(requested_sats, pconfig.get("max_amount_sats", 560000)), min_sats)

            await _tg(
                f"<b>Escrow-Flow{label}</b>\n"
                f"{requested_sats:,} sats (~{amount_fiat:.0f} {currency})\n\n"
                f"Schritt 1/2: Offer erstellen…",
                parse_mode="HTML")

            payment_methods = self._filter_payment_methods(
                pconfig.get("payment_methods", {"EUR": ["sepa", "revolut"]}), exclude_methods)

            offer = await loop.run_in_executor(None, lambda: peach.create_sell_offer(
                min_sats=min_sats, max_sats=max_sats,
                premium_pct=premium, payment_methods=payment_methods))
            escrow_addr = offer.escrow_address
            if not escrow_addr:
                try:
                    info = await loop.run_in_executor(None, lambda: peach.create_escrow(offer.id))
                    escrow_addr = info.get("address", "")
                except Exception as e:
                    log.warning(f"create_escrow: {e}")

            self.engine.add_pending_escrow(offer.id, escrow_addr, max_sats, premium)
            log.info(f"escrow: offer {offer.id} escrow={escrow_addr} amount={max_sats}")

            await _tg(
                f"✅ Offer: <code>{offer.id[:16]}</code> @ {premium}%\n"
                f"{min_sats:,}–{max_sats:,} sats\n\n"
                f"Schritt 2/2: Kraken-Withdrawal…",
                parse_mode="HTML")

            actual_balance = await loop.run_in_executor(None, exchange.get_btc_balance)
            withdrawal = await loop.run_in_executor(None, lambda: exchange.withdraw_btc("", actual_balance))
            log.info(f"escrow: withdrawal {withdrawal.withdrawal_id} ({actual_balance:.8f} BTC)")

            await _tg(
                f"✅ {actual_balance:.8f} BTC von {exchange.name} withdrawn\n"
                f"Withdrawal-ID: <code>{withdrawal.withdrawal_id}</code>\n\n"
                f"Warte auf On-Chain-Bestätigung…\n"
                f"(Automatische Escrow-Finanzierung sobald Hot Wallet UTXO verfügbar)",
                parse_mode="HTML")

            if not escrow_addr:
                await _tg(f"⚠️ Keine Escrow-Adresse für Offer <code>{offer.id}</code>", parse_mode="HTML"); return

            self._save_pending_funding(offer.id, escrow_addr, max_sats, hot_wallet_addr)
            threading.Thread(
                target=self._poll_and_fund_escrow,
                args=(offer.id, escrow_addr, max_sats, hot_wallet_addr),
                daemon=True, name=f"fund-{offer.id[:8]}").start()

        except Exception as e:
            await _tg(f"❌ Fehler: {str(e)[:400]}")
            log.exception(f"escrow: {e}")

    async def cmd_buy_escrow_norev(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Wie /buy_escrow, aber ohne Revolut als Zahlungsmethode"""
        await self._buy_escrow_with_methods(update, ctx, exclude_methods=["revolut"])

    async def _execute_buy_escrow(self, exchange, amount_fiat, manual_premium, send_fn, exclude_methods=None):
        """Execute the buy-escrow flow with a specific exchange."""
        currency = exchange.get_fiat_currency()
        peach = self.engine.platforms.get("peach")
        if not peach:
            await send_fn("Peach nicht verfügbar."); return
        pconfig = self.engine.config.get("platforms", {}).get("peach", {})
        hot_wallet_addr = pconfig.get("refund_address", "")
        if not hot_wallet_addr:
            await send_fn("Kein refund_address in config.json."); return

        async def _tg(text, **kw):
            """Send Telegram message, silently ignore rate-limit errors."""
            try:
                await send_fn(text, **kw)
            except Exception as tg_err:
                log.warning(f"TG send failed (flood control?): {tg_err}")

        label = " (ohne Revolut)" if exclude_methods and "revolut" in exclude_methods else ""
        await _tg(f"<b>Buy-Escrow-Flow{label}</b>\n{amount_fiat:.2f} {currency}\n\nSchritt 1/3: Spot-Preis…", parse_mode="HTML")
        loop = asyncio.get_event_loop()
        try:
            spot = await loop.run_in_executor(None, exchange.get_spot_price)
            withdraw_fee_sats = 0
            if hasattr(exchange, "get_withdrawal_fee_sats"):
                try:
                    withdraw_fee_sats = await loop.run_in_executor(None, exchange.get_withdrawal_fee_sats)
                except Exception:
                    withdraw_fee_sats = 15_000
            gross_sats = int((amount_fiat / spot) * 1e8)
            amount_sats = int(gross_sats * 0.99) - withdraw_fee_sats
            min_sats = pconfig.get("min_amount_sats", 10000)
            max_sats = max(min(amount_sats, pconfig.get("max_amount_sats", 560000)), min_sats)
            payment_methods = self._filter_payment_methods(
                pconfig.get("payment_methods", {"EUR": ["sepa", "revolut"]}), exclude_methods)
            premium = manual_premium if manual_premium is not None else self.engine.pricer.get_premium("peach", peach)

            offer = await loop.run_in_executor(None, lambda: peach.create_sell_offer(
                min_sats=min_sats, max_sats=max_sats,
                premium_pct=premium, payment_methods=payment_methods))
            escrow_addr = offer.escrow_address
            if not escrow_addr:
                try:
                    info = await loop.run_in_executor(None, lambda: peach.create_escrow(offer.id))
                    escrow_addr = info.get("address", "")
                except Exception as e:
                    log.warning(f"create_escrow: {e}")
            self.engine.add_pending_escrow(offer.id, escrow_addr, max_sats, premium)
            log.info(f"buy_escrow: offer {offer.id} escrow={escrow_addr} amount={max_sats}")

            # Check if hot wallet already has enough confirmed UTXOs — skip Kraken buy if so
            hot_wallet_sufficient = False
            try:
                from fund_from_wallet import get_utxos
                hw_utxos = await loop.run_in_executor(None, get_utxos, hot_wallet_addr)
                hw_confirmed = sum(u["value"] for u in hw_utxos if u.get("status", {}).get("confirmed", False))
                if hw_confirmed >= max_sats + 2000:
                    hot_wallet_sufficient = True
                    log.info(f"buy_escrow: hot wallet has {hw_confirmed:,} sats — skipping Kraken buy/withdraw")
            except Exception as hw_err:
                log.warning(f"buy_escrow: hot wallet check failed: {hw_err}")

            if hot_wallet_sufficient:
                await _tg(
                    f"✅ Offer: <code>{offer.id[:16]}</code> @ {premium}%\n{min_sats:,}–{max_sats:,} sats\n\n"
                    f"ℹ️ Hot Wallet hat genug ({hw_confirmed:,} sats) — Kraken-Kauf übersprungen\n\n"
                    f"Schritt 2/2: Hot Wallet → Escrow…",
                    parse_mode="HTML")
            else:
                await _tg(f"✅ Offer: <code>{offer.id[:16]}</code> @ {premium}%\n{min_sats:,}–{max_sats:,} sats\n\nSchritt 2/3: Kraken-Kauf…", parse_mode="HTML")

                buy = await loop.run_in_executor(None, exchange.buy_btc_market, amount_fiat)
                spot_at_buy = buy.effective_price if buy.effective_price else (buy.fiat_spent / buy.btc_amount if buy.btc_amount else 0)

                # Store actual buy data for accurate profit calculation later
                with self.engine._escrow_lock:
                    if offer.id in self.engine.pending_escrows:
                        self.engine.pending_escrows[offer.id]["buy_data"] = {
                            "fiat_spent": buy.fiat_spent,
                            "exchange_fee": buy.fee_fiat,
                            "spot_at_buy": spot_at_buy,
                            "btc_amount": buy.btc_amount,
                            "buy_currency": currency,
                        }

                # Withdraw to hot wallet, then fund escrow automatically
                actual_balance = await loop.run_in_executor(None, exchange.get_btc_balance)
                withdraw_amount = min(buy.btc_amount, actual_balance)
                withdrawal = await loop.run_in_executor(None, lambda: exchange.withdraw_btc("", withdraw_amount))
                log.info(f"buy_escrow: withdrawal {withdrawal.withdrawal_id} ({buy.btc_amount:.8f} BTC)")

                await _tg(
                    f"✅ {buy.btc_amount:.8f} BTC für {buy.fiat_spent:.2f} {currency}\n"
                    f"Withdrawal-ID: <code>{withdrawal.withdrawal_id}</code>\n\n"
                    f"Schritt 3/3: Warte auf On-Chain-Bestätigung…\n"
                    f"(Automatische Escrow-Finanzierung sobald Hot Wallet UTXO verfügbar)",
                    parse_mode="HTML")

            if not escrow_addr:
                await _tg(f"⚠️ Keine Escrow-Adresse für Offer <code>{offer.id}</code>", parse_mode="HTML"); return

            self._save_pending_funding(offer.id, escrow_addr, max_sats, hot_wallet_addr)
            threading.Thread(
                target=self._poll_and_fund_escrow,
                args=(offer.id, escrow_addr, max_sats, hot_wallet_addr),
                daemon=True, name=f"fund-{offer.id[:8]}").start()

        except Exception as e:
            await _tg(f"❌ Fehler: {str(e)[:400]}")
            log.exception(f"buy_escrow: {e}")

    def _save_pending_funding(self, offer_id, escrow_addr, amount_sats, hot_wallet_addr):
        with self._funding_lock:
            try:
                with open(self._fundings_file) as f:
                    data = json.load(f)
            except (FileNotFoundError, ValueError):
                data = []
            data = [d for d in data if d.get("offer_id") != offer_id]
            data.append({"offer_id": offer_id, "escrow_address": escrow_addr,
                         "amount_sats": amount_sats, "hot_wallet_addr": hot_wallet_addr})
            with open(self._fundings_file, "w") as f:
                json.dump(data, f)
        log.info(f"Saved pending funding for {offer_id[:12]}")

    def _remove_pending_funding(self, offer_id):
        with self._funding_lock:
            try:
                with open(self._fundings_file) as f:
                    data = json.load(f)
                data = [d for d in data if d.get("offer_id") != offer_id]
                with open(self._fundings_file, "w") as f:
                    json.dump(data, f)
            except Exception:
                pass

    def _resume_pending_fundings(self):
        try:
            with open(self._fundings_file) as f:
                data = json.load(f)
        except (FileNotFoundError, ValueError):
            return
        if not data:
            return
        log.info(f"Resuming {len(data)} pending wallet funding(s) after restart...")
        for entry in data:
            offer_id = entry.get("offer_id", "")
            escrow_addr = entry.get("escrow_address", "")
            amount_sats = entry.get("amount_sats", 0)
            hot_wallet_addr = entry.get("hot_wallet_addr", "")
            if not all([offer_id, escrow_addr, amount_sats, hot_wallet_addr]):
                continue
            log.info(f"Resuming funding poll for {offer_id[:12]}")
            threading.Thread(
                target=self._poll_and_fund_escrow,
                args=(offer_id, escrow_addr, amount_sats, hot_wallet_addr),
                daemon=True, name=f"fund-{offer_id[:8]}").start()

    def _poll_and_fund_escrow(self, offer_id, escrow_addr, amount_sats, hot_wallet_addr):
        """Background-Thread: warte auf UTXO in Hot Wallet, finanziere dann Escrow"""
        import time
        from fund_from_wallet import get_utxos, fund_escrow
        max_wait = 14400  # 4h
        fifo_stale_timeout = 7200  # 2h — treat predecessor as dead if stuck this long
        topup_after = 300   # 5 min of insufficient balance → auto top-up
        start = time.time()
        _insufficient_since = None
        _topup_triggered = False

        # Record when this thread started so FIFO can detect dead threads
        with self.engine._escrow_lock:
            if offer_id in self.engine.pending_escrows:
                self.engine.pending_escrows[offer_id]["funding_started_at"] = start

        log.info(f"Polling hot wallet für Escrow {offer_id[:12]} ({amount_sats} sats)")
        while time.time() - start < max_wait:
            time.sleep(60)
            try:
                # Check escrow/offer status to prevent double-funding or funding dead offers
                platform = None
                for p in self.engine.platforms.values():
                    if hasattr(p, 'get_escrow_status'):
                        platform = p
                        break
                if platform:
                    try:
                        escrow_info = platform.get_escrow_status(offer_id, use_cache=False)
                        funding_status = escrow_info.get('funding', {}).get('status', '') if isinstance(escrow_info.get('funding'), dict) else escrow_info.get('funding', '')
                        if funding_status in ('FUNDED', 'MEMPOOL'):
                            log.info(f"Poll fund {offer_id[:12]}: escrow already {funding_status}, stopping poll")
                            with self.engine._escrow_lock:
                                if offer_id in self.engine.pending_escrows:
                                    self.engine.pending_escrows[offer_id]["funded"] = True
                                    self.engine.pending_escrows[offer_id].pop("funding_in_progress", None)
                            self._remove_pending_funding(offer_id)
                            return
                        if funding_status == 'WRONG_FUNDING_AMOUNT':
                            log.error(f"Poll fund {offer_id[:12]}: WRONG_FUNDING_AMOUNT! Manual check needed")
                            self._remove_pending_funding(offer_id)
                            if self.notifier:
                                self.notifier._send(f"🚨 <b>WRONG_FUNDING_AMOUNT</b>\nOffer: <code>{offer_id}</code>\nManuelle Prüfung nötig!")
                            return
                    except Exception as e:
                        log.warning(f"Poll fund {offer_id[:12]}: escrow status check failed: {e}, skipping this cycle")
                        continue

                # FIFO: only the first inserted pending offer funds; later ones wait.
                # Dead-thread guard: skip a predecessor that has been stuck > fifo_stale_timeout.
                with self.engine._escrow_lock:
                    first_pending = None
                    for oid, info in self.engine.pending_escrows.items():
                        if not info.get("funded") and info.get("funding_in_progress"):
                            started = info.get("funding_started_at", time.time())
                            if time.time() - started > fifo_stale_timeout:
                                log.warning(f"Poll fund: skipping stale predecessor {oid[:12]} (stuck >{fifo_stale_timeout//3600}h)")
                                continue
                            first_pending = oid
                            break
                if first_pending and first_pending != offer_id:
                    log.debug(f"Poll fund {offer_id[:12]}: waiting for {first_pending[:12]} to fund first")
                    continue

                utxos = get_utxos(hot_wallet_addr)
                confirmed = [u for u in utxos if u.get('status', {}).get('confirmed', False)]
                available = sum(u["value"] for u in (confirmed or utxos))
                try:
                    from fund_from_wallet import get_fee_rate
                    fee_buffer = max(150 * get_fee_rate(), 1500)
                except Exception:
                    fee_buffer = 3000
                log.info(f"Hot Wallet: {available} sats verfügbar (brauche {amount_sats} + ~{fee_buffer} fee)")

                if available >= amount_sats + fee_buffer:
                    _insufficient_since = None
                    result = fund_escrow(self.engine.config, escrow_addr, amount_sats)
                    with self.engine._escrow_lock:
                        if offer_id in self.engine.pending_escrows:
                            self.engine.pending_escrows[offer_id]["funded"] = True
                            self.engine.pending_escrows[offer_id]["funded_at"] = __import__('datetime').datetime.now().isoformat()
                            self.engine.pending_escrows[offer_id].pop("funding_in_progress", None)
                            self.engine.pending_escrows[offer_id].setdefault("buy_data", {})["funding_fee_sats"] = result.get("fee", 0)
                    self._remove_pending_funding(offer_id)
                    if self.notifier:
                        self.notifier._send(
                            f"<b>Escrow finanziert!</b>\n"
                            f"Offer: <code>{offer_id[:16]}</code>\n"
                            f"TXID: <code>{result['txid']}</code>\n"
                            f"Fee: {result['fee']} sats")
                    return
                else:
                    # Insufficient balance — track duration for auto top-up
                    if _insufficient_since is None:
                        _insufficient_since = time.time()
                    elif not _topup_triggered and time.time() - _insufficient_since >= topup_after:
                        deficit_sats = amount_sats + fee_buffer - available
                        self._try_topup_for_escrow(offer_id, deficit_sats)
                        _topup_triggered = True  # only once per poll session

            except Exception as e:
                log.warning(f"Poll fund {offer_id[:12]}: {e}")
        # Clear funding_in_progress so engine can pick it up if it gets funded externally
        with self.engine._escrow_lock:
            if offer_id in self.engine.pending_escrows:
                self.engine.pending_escrows[offer_id].pop("funding_in_progress", None)
        log.warning(f"Poll fund {offer_id[:12]}: timeout after {max_wait}s, will retry on restart")
        if self.notifier:
            self.notifier._send(
                f"⚠️ <b>Timeout</b>: Escrow nach {max_wait//3600}h nicht finanziert (wird bei Restart erneut versucht)\n"
                f"Offer: <code>{offer_id}</code> | {amount_sats:,} sats")

    def _try_topup_for_escrow(self, offer_id, deficit_sats):
        """Buy the deficit amount on Kraken and withdraw to hot wallet to cover a stuck escrow."""
        exchange = self.engine.get_best_exchange()
        if not exchange:
            log.warning(f"topup {offer_id[:12]}: no exchange available")
            return
        try:
            spot = exchange.get_spot_price()
            currency = exchange.get_fiat_currency() if hasattr(exchange, "get_fiat_currency") else "?"
            # Add 5% buffer on top of deficit to account for withdrawal fee and rounding
            topup_sats = int(deficit_sats * 1.05) + 15_000
            topup_fiat = topup_sats / 1e8 * spot
            min_buy = 10.0  # don't buy less than 10 fiat
            topup_fiat = max(topup_fiat, min_buy)
            log.info(f"topup {offer_id[:12]}: buying {topup_fiat:.2f} {currency} to cover {deficit_sats:,} sats deficit")
            if self.notifier:
                self.notifier._send(
                    f"⚠️ <b>Auto Top-Up</b>\n"
                    f"Offer <code>{offer_id[:16]}</code> hat Unterdeckung ({deficit_sats:,} sats)\n"
                    f"Kaufe {topup_fiat:.2f} {currency} nach…")
            buy = exchange.buy_btc_market(topup_fiat)
            withdrawal = exchange.withdraw_btc("", buy.btc_amount)
            log.info(f"topup {offer_id[:12]}: withdrawal {withdrawal.withdrawal_id} ({buy.btc_amount:.8f} BTC)")
            if self.notifier:
                self.notifier._send(
                    f"✅ <b>Top-Up gesendet</b>\n"
                    f"{buy.btc_amount:.8f} BTC → Hot Wallet\n"
                    f"Withdrawal: <code>{withdrawal.withdrawal_id}</code>")
        except Exception as e:
            log.error(f"topup {offer_id[:12]}: {e}")
            if self.notifier:
                self.notifier._send(f"❌ <b>Top-Up fehlgeschlagen</b>\nOffer: <code>{offer_id[:16]}</code>\n{str(e)[:200]}")

    async def cmd_buy(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Kaufe BTC auf Kraken und sende zur Hot Wallet"""
        if not self._auth(update) or not self.engine: return
        args = ctx.args
        if not args:
            await update.message.reply_text("Verwendung: /buy &lt;betrag&gt;\nBeispiel: /buy 200", parse_mode="HTML"); return
        try:
            amount = float(args[0])
        except ValueError:
            await update.message.reply_text("Ungültiger Betrag."); return
        if amount <= 0 or amount > 10000:
            await update.message.reply_text("Betrag muss zwischen 1 und 10'000 sein."); return

        # Check if multiple currencies available
        currencies = self._get_available_currencies()
        if len(currencies) > 1:
            self._pending_buy_params[str(update.effective_chat.id)] = {
                "command": "buy", "amount": amount
            }
            buttons = [[InlineKeyboardButton(f"{c}", callback_data=f"buy_{c.lower()}") for c in currencies]]
            await update.message.reply_text(
                f"<b>{amount:.0f} — Währung wählen:</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
            return

        exchange = self.engine.get_best_exchange()
        if not exchange:
            await update.message.reply_text("Kein Exchange verfügbar."); return
        await self._execute_buy(exchange, amount, send_fn=update.message.reply_text)

    async def _execute_buy(self, exchange, amount, send_fn):
        """Execute buy+withdraw with a specific exchange."""
        currency = exchange.get_fiat_currency()
        await send_fn(f"Kaufe {amount:.2f} {currency} BTC auf {exchange.name}...")
        loop = asyncio.get_event_loop()
        try:
            buy = await loop.run_in_executor(None, exchange.buy_btc_market, amount)
            withdrawal = await loop.run_in_executor(None, lambda: exchange.withdraw_btc("", buy.btc_amount))
            await send_fn(
                f"<b>Gekauft &amp; gesendet</b>\n"
                f"{buy.btc_amount:.8f} BTC für {buy.fiat_spent:.2f} {currency}\n"
                f"Fee: {buy.fee_fiat:.4f} {currency}\n"
                f"Withdrawal-ID: <code>{withdrawal.withdrawal_id}</code>",
                parse_mode="HTML")
        except Exception as e:
            await send_fn(f"Fehler: {e}")

    async def cmd_wallet(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Hot Wallet Balance anzeigen — gesamt, allocated, frei"""
        if not self._auth(update) or not self.engine: return
        pconfig = self.engine.config.get("platforms", {}).get("peach", {})
        addr = pconfig.get("refund_address", "")
        if not addr:
            await update.message.reply_text("Kein refund_address in config.json."); return
        await update.message.reply_text("Lade Hot Wallet…")
        loop = asyncio.get_event_loop()
        try:
            from fund_from_wallet import get_utxos
            from core.engine import SpotPriceProvider
            utxos = await loop.run_in_executor(None, get_utxos, addr)
            confirmed = [u for u in utxos if u.get("status", {}).get("confirmed", False)]
            unconfirmed = [u for u in utxos if not u.get("status", {}).get("confirmed", False)]
            total_conf = sum(u["value"] for u in confirmed)
            total_unconf = sum(u["value"] for u in unconfirmed)

            try:
                spot = await loop.run_in_executor(None, SpotPriceProvider.get_spot_chf)
                def chf(sats): return f" ≈ {sats / 1e8 * spot:,.2f} CHF"
            except Exception:
                def chf(sats): return ""

            # Allocated: pending unfunded escrows
            with self.engine._escrow_lock:
                pending_unfunded = [
                    (oid, info) for oid, info in self.engine.pending_escrows.items()
                    if not info.get("funded")
                ]
            allocated_sats = sum(info["amount_sats"] for _, info in pending_unfunded)
            free_sats = total_conf - allocated_sats

            lines = [f"<b>Hot Wallet</b>\n<code>{addr}</code>\n"]

            lines.append(f"Confirmed:   <b>{total_conf:,} sats</b>{chf(total_conf)}")
            if total_unconf:
                lines.append(f"Unconfirmed: {total_unconf:,} sats{chf(total_unconf)} ⏳")

            lines.append("")
            if pending_unfunded:
                lines.append(f"Allocated:   <b>{allocated_sats:,} sats</b>{chf(allocated_sats)}")
                for oid, info in pending_unfunded:
                    status = "⏳ funding" if info.get("funding_in_progress") else "⚠️ waiting"
                    deficit = info["amount_sats"] - total_conf
                    deficit_str = f" <i>(−{deficit:,} sats fehlen)</i>" if deficit > 0 else ""
                    lines.append(f"  • <code>{oid[:16]}</code> {info['amount_sats']:,} sats {status}{deficit_str}")
                if free_sats >= 0:
                    lines.append(f"Frei:        <b>{free_sats:,} sats</b>{chf(free_sats)}")
                else:
                    lines.append(f"Frei:        ⚠️ <b>−{abs(free_sats):,} sats</b> (Unterdeckung)")
            else:
                lines.append(f"Frei:        <b>{free_sats:,} sats</b>{chf(free_sats)}")

            if utxos:
                lines.append(f"\n<b>UTXOs</b> ({len(confirmed)} confirmed, {len(unconfirmed)} unconfirmed)")
                for u in utxos[:8]:
                    icon = "✓" if u.get("status", {}).get("confirmed") else "⏳"
                    lines.append(f"  {icon} <code>{u['txid'][:12]}…</code>:{u['vout']} | {u['value']:,} sats")
                if len(utxos) > 8:
                    lines.append(f"  … und {len(utxos)-8} weitere")

            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"Fehler: {e}")

    async def cmd_contracts(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Aktive Peach-Contracts anzeigen"""
        if not self._auth(update) or not self.engine: return
        lines = ["<b>Contracts</b>\n"]
        found = False
        loop = asyncio.get_event_loop()
        for n, p in self.engine.platforms.items():
            try:
                all_contracts = await loop.run_in_executor(None, p.get_contracts)
                # Only show active contracts (not completed/cancelled/stale)
                _ignore_offers = {"335679", "336216", "336271", "336362", "336380"}
                contracts = [c for c in all_contracts if c.status not in (
                    OfferStatus.COMPLETED, OfferStatus.CANCELLED)
                    and c.offer_id not in _ignore_offers]
                if not contracts:
                    lines.append(f"<b>{n}</b>: keine")
                    continue
                found = True
                lines.append(f"<b>{n}</b> ({len(contracts)}):")
                for c in contracts:
                    status_label = c.status.value if hasattr(c.status, 'value') else str(c.status)
                    sats = f"{c.amount_sats:,}" if c.amount_sats else "?"
                    fiat = f"{c.price_fiat} {c.currency}" if c.price_fiat else ""
                    lines.append(f"\n  <code>{c.id[:16]}</code>")
                    lines.append(f"  Status: {status_label}")
                    lines.append(f"  {sats} sats | {fiat}")
                    if c.payment_method:
                        lines.append(f"  Methode: {c.payment_method}")
                    if status_label == "payment_received":
                        lines.append(f"  → In Peach App bestätigen")
            except Exception as e:
                lines.append(f"<b>{n}</b>: {e}")
        if not found:
            lines.append("Keine aktiven Contracts.")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def cmd_cancel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Peach-Offer abbrechen"""
        if not self._auth(update) or not self.engine: return
        args = ctx.args
        if not args:
            await update.message.reply_text(
                "Verwendung: /cancel &lt;offer_id&gt;\nOffers anzeigen: /offers",
                parse_mode="HTML"); return
        offer_id = args[0]
        peach = self.engine.platforms.get("peach")
        if not peach:
            await update.message.reply_text("Peach nicht verfügbar."); return
        loop = asyncio.get_event_loop()
        try:
            # Register refund monitoring BEFORE cancel (need escrow info)
            await loop.run_in_executor(None, self.engine.register_refund, offer_id)
            await loop.run_in_executor(None, peach.cancel_offer, offer_id)
            with self.engine._escrow_lock:
                if offer_id in self.engine.pending_escrows:
                    del self.engine.pending_escrows[offer_id]
            await update.message.reply_text(
                f"<b>Offer abgebrochen</b>\n<code>{offer_id[:16]}</code>",
                parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"Fehler: {e}")

    async def cmd_sell(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """BTC auf Kraken verkaufen. Verwendung: /sell <btc_amount>"""
        if not self._auth(update) or not self.engine: return
        args = ctx.args
        if not args:
            await update.message.reply_text(
                "Verwendung: /sell &lt;btc_menge&gt;\nBeispiel: /sell 0.005\n         /sell all",
                parse_mode="HTML"); return
        loop = asyncio.get_event_loop()
        if args[0].lower() == "all":
            # Sell all BTC — need to pick exchange first
            pass
        else:
            try:
                amount_btc = float(args[0])
            except ValueError:
                await update.message.reply_text("Ungültige BTC-Menge."); return
            if amount_btc <= 0:
                await update.message.reply_text("Menge muss > 0 sein."); return

        currencies = self._get_available_currencies()
        if len(currencies) > 1:
            self._pending_buy_params[str(update.effective_chat.id)] = {
                "command": "sell", "amount_btc": float(args[0]) if args[0].lower() != "all" else "all"
            }
            buttons = [[InlineKeyboardButton(f"{c}", callback_data=f"sell_{c.lower()}") for c in currencies]]
            await update.message.reply_text(
                f"<b>Verkaufen — Währung wählen:</b>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
            return

        exchange = self.engine.get_best_exchange()
        if not exchange:
            await update.message.reply_text("Kein Exchange verfügbar."); return
        btc = amount_btc if args[0].lower() != "all" else await loop.run_in_executor(None, exchange.get_btc_balance)
        await self._execute_sell(exchange, btc, send_fn=update.message.reply_text)

    async def _execute_sell(self, exchange, amount_btc, send_fn):
        """Execute sell on exchange."""
        currency = exchange.get_fiat_currency()
        if amount_btc == "all":
            loop = asyncio.get_event_loop()
            amount_btc = await loop.run_in_executor(None, exchange.get_btc_balance)
        if not amount_btc or amount_btc <= 0:
            await send_fn("Kein BTC-Guthaben zum Verkaufen."); return
        await send_fn(f"Verkaufe {amount_btc:.8f} BTC auf {exchange.name}...")
        loop = asyncio.get_event_loop()
        try:
            sell = await loop.run_in_executor(None, exchange.sell_btc_market, amount_btc)
            await send_fn(
                f"<b>Verkauft</b>\n"
                f"{sell.btc_sold:.8f} BTC → {sell.fiat_received:.2f} {currency}\n"
                f"Fee: {sell.fee_fiat:.4f} {currency}\n"
                f"Preis: {sell.effective_price:.2f} {currency}/BTC",
                parse_mode="HTML")
        except Exception as e:
            await send_fn(f"Fehler: {e}")

    async def cmd_reload(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Lädt config.json neu — inkl. Exchange-Keys und Peach-Credentials"""
        if not self._auth(update) or not self.engine: return
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.engine.reload_config)
            await update.message.reply_text(
                "✅ <b>Config neu geladen</b>\n"
                "Exchange-Keys, Peach-Credentials und auto_buy_escrow aktualisiert.",
                parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"❌ Fehler beim Laden: {e}")
            log.warning(f"Config reload failed: {e}")

    async def cmd_refunds(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Zeige pending Refunds"""
        if not self._auth(update) or not self.engine: return
        refunds = self.engine._pending_refunds
        if not refunds:
            await update.message.reply_text("Keine offenen Refunds."); return
        total = sum(v["amount_sats"] for v in refunds.values())
        lines = [f"<b>Offene Refunds</b> ({len(refunds)})\n"]
        for oid, info in refunds.items():
            lines.append(f"• {oid}: {info['amount_sats']:,} sats")
        lines.append(f"\n<b>Total: {total:,} sats</b>")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    def build_app(self):
        app = Application.builder().token(self.token).build()
        for cmd, fn in [("start",self.cmd_start),("status",self.cmd_status),("balance",self.cmd_balance),("offers",self.cmd_offers),("pause",self.cmd_pause),("resume",self.cmd_resume),("profit",self.cmd_profit),("market",self.cmd_market),("buy",self.cmd_buy),("escrow",self.cmd_escrow),("escrow_norev",self.cmd_escrow_norev),("fund",self.cmd_fund),("fund_norev",self.cmd_fund_norev),("buy_escrow",self.cmd_buy_escrow),("buy_escrow_norev",self.cmd_buy_escrow_norev),("wallet",self.cmd_wallet),("contracts",self.cmd_contracts),("sell",self.cmd_sell),("cancel",self.cmd_cancel),("refunds",self.cmd_refunds),("reload",self.cmd_reload)]:
            app.add_handler(CommandHandler(cmd, fn))
        app.add_handler(CallbackQueryHandler(self.handle_callback))
        return app
    def start_in_thread(self):
        self._resume_pending_fundings()
        def _run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            app = self.build_app()
            self.notifier.set_app(app, loop)
            loop.run_until_complete(app.initialize())
            loop.run_until_complete(app.start())
            loop.run_until_complete(app.updater.start_polling(drop_pending_updates=True))
            log.info("Telegram running.")
            loop.run_forever()
        t = threading.Thread(target=_run, daemon=True, name="telegram")
        t.start()
        return t
    def run_polling(self):
        app = self.build_app()
        app.run_polling(drop_pending_updates=True)
