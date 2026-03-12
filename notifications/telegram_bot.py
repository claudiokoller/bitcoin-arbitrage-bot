"""
Telegram Bot – Notifications & Remote Control.

Provides a full trading interface via Telegram:
  - /status, /balance, /offers – Monitor bot state
  - /buy_escrow <fiat> [premium%] – Full cycle: Kraken buy → Offer → Escrow
  - /market – Competitive analysis with premium recommendations
  - /contracts, /cancel, /refunds – Trade management
  - Inline keyboards for quick actions (pause/resume, cancel offers)
  - Background escrow funding with UTXO polling
"""
import asyncio, json, logging, os, threading
from datetime import datetime
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
log = logging.getLogger("bot.telegram")


class TelegramNotifier:
    """Async-safe notification sender for the trading engine."""

    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self._loop = None
        self._app = None

    def set_app(self, app, loop):
        self._app = app
        self._loop = loop

    def _send(self, text):
        if not self._app or not self._loop:
            return
        future = asyncio.run_coroutine_threadsafe(
            self._app.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="HTML"),
            self._loop
        )
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
        self._send(f"<b>Today:</b> {s['count']} Trades | {s['total_profit']:.2f} CHF")


class TelegramBot:
    """Full Telegram bot with trading commands and inline keyboards."""

    def __init__(self, token, chat_id, engine=None):
        self.token = token
        self.chat_id = str(chat_id)
        self.engine = engine
        self.notifier = TelegramNotifier(token, chat_id)
        self._funding_lock = threading.Lock()

    def _auth(self, update):
        return str(update.effective_chat.id) == self.chat_id

    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        cid = update.effective_chat.id
        if str(cid) != self.chat_id:
            await update.message.reply_text(
                f"Chat-ID: <code>{cid}</code>", parse_mode="HTML")
            return
        await update.message.reply_text(
            "<b>Trading Bot</b>\n\n"
            "/status /balance /offers\n"
            "/pause /resume\n"
            "/trades /profit /market\n\n"
            "<b>Buy</b>\n"
            "/buy_escrow &lt;fiat&gt; [premium%]\n"
            "/buy &lt;fiat&gt;\n\n"
            "<b>Management</b>\n"
            "/wallet /contracts /cancel /refunds",
            parse_mode="HTML")

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show bot status with inline keyboard for pause/resume."""
        if not self._auth(update) or not self.engine:
            return
        s = self.engine.get_status()
        paused = "PAUSED" if s["paused"] else "ACTIVE"
        text = (
            f"<b>Status</b> {paused} ({s.get('uptime', '?')})\n\n"
            f"Offers: {s['pending_escrows']} pending, {s['funded_escrows']} funded\n"
            f"Volume: {s['daily_volume_sats']:,} sats"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Refresh", callback_data="refresh_status"),
            InlineKeyboardButton(
                "Pause" if not s["paused"] else "Resume",
                callback_data="toggle_pause"
            )
        ]])
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)

    async def cmd_buy_escrow(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Full cycle: Kraken market buy -> create Peach offer -> fund escrow.

        Steps:
        1. Get spot price, calculate sats amount
        2. Create sell offer on Peach with payment methods + premium
        3. Market buy BTC on Kraken
        4. Withdraw to hot wallet
        5. Background thread polls for UTXO, then funds escrow on-chain

        Implementation handles:
        - Escrow double-funding prevention
        - Buy data preservation for profit tracking
        - Automatic retry on withdrawal
        - Persistent pending fundings (survives restart)
        """
        raise NotImplementedError("Buy-escrow flow - see private repo")

    async def cmd_market(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Competitive market analysis with premium recommendation.

        Scans Peach P2P market for:
        - Seller competition by price range and payment method
        - Buyer demand distribution
        - Premium distribution in target range (100-300 CHF)
        - Optimal premium: undercuts lowest 3 competitors by 0.5%
        - Optimal amount: highest buyer/seller ratio sub-range
        """
        raise NotImplementedError("Market analysis - see private repo")

    async def cmd_balance(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine:
            return
        lines = ["<b>Balances</b>\n"]
        for n, ex in self.engine.exchanges.items():
            try:
                s = ex.get_status()
                lines.append(f"<b>{n}</b>: {s.get('fiat_balance',0):,.2f} {s.get('currency','CHF')} | "
                             f"{s.get('btc_balance',0):.8f} BTC")
            except Exception as e:
                lines.append(f"<b>{n}</b>: {e}")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def cmd_offers(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """List active offers with inline cancel buttons."""
        if not self._auth(update) or not self.engine:
            return
        lines = ["<b>Offers</b>\n"]
        buttons = []
        for n, p in self.engine.platforms.items():
            try:
                offers = p.get_active_offers()
                if not offers:
                    lines.append(f"<b>{n}</b>: none"); continue
                lines.append(f"<b>{n}</b> ({len(offers)}):")
                for o in offers:
                    lines.append(f"  {o.id} | {o.premium_pct}% | {o.status.value}")
                    buttons.append([InlineKeyboardButton(
                        f"Cancel {o.id}", callback_data=f"cancel_{o.id}")])
            except Exception as e:
                lines.append(f"<b>{n}</b>: {e}")
        markup = InlineKeyboardMarkup(buttons) if buttons else None
        await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=markup)

    async def cmd_pause(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if self._auth(update) and self.engine:
            self.engine.paused = True
            await update.message.reply_text("<b>Paused</b>", parse_mode="HTML")

    async def cmd_resume(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if self._auth(update) and self.engine:
            self.engine.paused = False
            await update.message.reply_text("<b>Running</b>", parse_mode="HTML")

    async def cmd_trades(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine:
            return
        trades = self.engine.trade_logger.get_recent(10)
        if not trades:
            await update.message.reply_text("No trades."); return
        lines = ["<b>Trades</b>\n"]
        for t in trades:
            lines.append(f"{t['timestamp'][:16]} | {t['amount_sats']:,} sats | "
                         f"<b>{t['net_profit']:+.2f} CHF</b>")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def cmd_profit(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update) or not self.engine:
            return
        d = self.engine.trade_logger.get_daily_summary()
        m = self.engine.trade_logger.get_period_summary(30)
        text = (f"<b>Profit</b>\n"
                f"Today: {d['count']} | {d['total_profit']:.2f} CHF\n"
                f"30d: {m['count']} | {m['total_profit']:.2f} CHF")
        await update.message.reply_text(text, parse_mode="HTML")

    async def handle_callback(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Handle inline keyboard callbacks (pause/resume, cancel offers)."""
        q = update.callback_query
        await q.answer()
        if str(q.message.chat.id) != self.chat_id:
            return
        if q.data == "toggle_pause":
            self.engine.paused = not self.engine.paused
            await q.edit_message_text(
                f"Bot: {'paused' if self.engine.paused else 'running'}", parse_mode="HTML")
        elif q.data == "refresh_status":
            update.message = q.message
            await self.cmd_status(update, ctx)
        elif q.data.startswith("cancel_"):
            # Two-step cancel: confirmation required
            offer_id = q.data.replace("cancel_", "")
            buttons = [[InlineKeyboardButton(
                f"Confirm cancel", callback_data=f"confirm_cancel_{offer_id}")]]
            await q.edit_message_text(
                f"Cancel <code>{offer_id}</code>?",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))

    def build_app(self):
        app = Application.builder().token(self.token).build()
        commands = [
            ("start", self.cmd_start), ("status", self.cmd_status),
            ("balance", self.cmd_balance), ("offers", self.cmd_offers),
            ("pause", self.cmd_pause), ("resume", self.cmd_resume),
            ("trades", self.cmd_trades), ("profit", self.cmd_profit),
            ("market", self.cmd_market), ("buy_escrow", self.cmd_buy_escrow),
        ]
        for cmd, fn in commands:
            app.add_handler(CommandHandler(cmd, fn))
        app.add_handler(CallbackQueryHandler(self.handle_callback))
        return app

    def start_in_thread(self):
        """Start Telegram bot in background thread."""
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
