import os
import sys
import time
import sqlite3
import threading
import pandas as pd
import requests
import ccxt
import asyncio
import html
import json

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify

# =================================================================
# CONFIGURATION & PARAMETERS
# =================================================================
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

SL_BUFFER_PERCENT = 0.0005
MAX_DAILY_SL = 2
DB_FILE = "trades.db"
WIB = timezone(timedelta(hours=7))

MANUAL_ALERT_PAIRS = ['XAU/USDT', 'EUR/USDT', 'GBP/USDT', 'USOIL/USDT', 'WTI/USDT', 'USO/USDT']
CORE_PAIRS = ['BTC/USDT', 'ETH/USDT']

DEFAULT_CEX = {
    'bingx': {'type': 'swap'},
    'bitget': {'type': 'swap'},
    'bybit': {'type': 'linear'},
    'binance': {'type': 'future'}
}

exchanges = {}

CONFIG = {
    'MARGIN_PER_TRADE': 2.0,
    'MAX_CONCURRENT_POSITIONS': 2,
    'LEVERAGE': {ex: 50 for ex in DEFAULT_CEX.keys()},
    'DRY_RUN': True,
    'MIN_TRADES': 5,
    'MIN_WR': 40.0
}

IS_BOT_PAUSED = False
daily_sl_count = 0
DAILY_LOSS_ENABLED = True
last_error_time = 0
ERROR_COOLDOWN = 300
last_runtime_status = None

# =================================================================
# DATABASE & SETTINGS
# =================================================================
def init_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        exchange_id TEXT,
        symbol TEXT,
        order_type TEXT,
        side TEXT,
        entry_price REAL,
        sl_price REAL,
        tp_price REAL,
        order_id TEXT,
        bars_count INTEGER DEFAULT 0,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value REAL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS cex_settings (
        ex_id TEXT PRIMARY KEY,
        api_key TEXT,
        secret TEXT,
        password TEXT DEFAULT '',
        enabled INTEGER DEFAULT 0,
        account_label TEXT DEFAULT ''
    )''')
    try:
        c.execute("ALTER TABLE cex_settings ADD COLUMN enabled INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE cex_settings ADD COLUMN account_label TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE cex_settings ADD COLUMN account_info TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE cex_settings ADD COLUMN password TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass

    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('margin', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('max_pos', 2.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('dry_run', 1.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('min_trades', 5.0)")
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('min_wr', 40.0)")

    for ex in DEFAULT_CEX.keys():
        c.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, 50.0)",
            (f'lev_{ex}',)
        )
        c.execute(
            "INSERT OR IGNORE INTO cex_settings (ex_id, api_key, secret, password, enabled, account_label, account_info) VALUES (?, '', '', '', 0, '', '')",
            (ex,)
        )

    conn.commit()
    conn.close()


def load_settings():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()

    for row in c.execute("SELECT key, value FROM settings").fetchall():
        if row[0] == 'margin':
            CONFIG['MARGIN_PER_TRADE'] = float(row[1])
        elif row[0] == 'max_pos':
            CONFIG['MAX_CONCURRENT_POSITIONS'] = int(row[1])
        elif row[0] == 'dry_run':
            CONFIG['DRY_RUN'] = bool(row[1])
        elif row[0] == 'min_trades':
            CONFIG['MIN_TRADES'] = int(row[1])
        elif row[0] == 'min_wr':
            CONFIG['MIN_WR'] = float(row[1])
        elif row[0].startswith('lev_'):
            CONFIG['LEVERAGE'][row[0].split('_', 1)[1]] = int(row[1])

    conn.close()


def reload_exchanges():
    global exchanges

    conn = sqlite3.connect(DB_FILE, timeout=10)
    rows = conn.cursor().execute(
        "SELECT ex_id, api_key, secret, password, enabled FROM cex_settings"
    ).fetchall()
    conn.close()

    exchanges.clear()

    for ex_id, api, sec, password, en in rows:
        if en == 1:
            try:
                params = {
                    'apiKey': api if api else '',
                    'secret': sec if sec else '',
                    'options': {'defaultType': DEFAULT_CEX[ex_id]['type']},
                    'enableRateLimit': True,
                    'timeout': 5000
                }
                if ex_id == 'bitget':
                    params['password'] = password if password else ''
                exchanges[ex_id] = getattr(ccxt, ex_id)(params)
            except Exception:
                pass


def update_db_setting(key, value):
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.cursor().execute(
        "UPDATE settings SET value=? WHERE key=?",
        (value, key)
    )
    conn.commit()
    conn.close()


# =================================================================
# HELPERS
# =================================================================

def get_db_setting(key, default=1.0):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        row = conn.cursor().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return float(row[0]) if row else default
    except Exception:
        return default

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code != 200:
            payload.pop("parse_mode", None)
            requests.post(url, json=payload, timeout=5)
    except Exception:
        pass


def send_error_alert(err_msg):
    global last_error_time
    if time.time() - last_error_time > ERROR_COOLDOWN:
        safe_err = html.escape(str(err_msg)[:3500])
        send_telegram(f"⚠️ <b>ERROR ALERT</b>\n\n<code>{safe_err}</code>")
        last_error_time = time.time()


def is_kill_zone():
    h = datetime.now(WIB).hour
    return (14 <= h < 17) or (19 <= h < 22)


def is_manual_alert_symbol(symbol):
    return any(p in symbol.split(':')[0].upper() for p in MANUAL_ALERT_PAIRS)


def get_free_usdt_balance(ex_instance):
    try:
        bal = ex_instance.fetch_balance()
        usdt = bal.get('USDT', {})
        if isinstance(usdt, dict):
            return float(usdt.get('free', 0.0))
        return 0.0
    except Exception:
        return 0.0


def cancel_all_pending_orders_on_pause():
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        c = conn.cursor()
        rows = c.execute(
            "SELECT id, exchange_id, symbol, order_id "
            "FROM trades WHERE status='PENDING'"
        ).fetchall()

        for t_id, ex_id, sym, ord_id in rows:
            if ex_id in exchanges and not str(ord_id).startswith('sim_'):
                try:
                    exchanges[ex_id].cancel_order(ord_id, sym)
                except Exception:
                    pass
            c.execute(
                "UPDATE trades SET status='CANCELLED (Auto/Manual)' WHERE id=?",
                (t_id,)
            )

        conn.commit()
        conn.close()
        send_telegram("🧹 <b>CLEANUP</b> Semua pending order dibatalkan!")
    except Exception:
        pass


def check_and_update_daily_sl():
    global daily_sl_count, IS_BOT_PAUSED
    if not DAILY_LOSS_ENABLED:
        return
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        daily_sl_count = conn.cursor().execute(
            "SELECT COUNT(*) FROM trades "
            "WHERE created_at >= date('now', 'start of day') "
            "AND status LIKE '%SL%'"
        ).fetchone()[0]
        conn.close()

        if daily_sl_count >= MAX_DAILY_SL and not IS_BOT_PAUSED:
            IS_BOT_PAUSED = True
            cancel_all_pending_orders_on_pause()
            send_telegram(
                f"🛑 <b>DAILY LOSS LIMIT</b>\n"
                f"Kena {daily_sl_count}x SL. Bot PAUSE!"
            )
    except Exception:
        pass


# =================================================================
# SMC STRATEGY & LIFECYCLE
# =================================================================
# LOGIC TRADE TIDAK DIUBAH
def analyze_smc_retrace(df_m15, df_h1, daily_open):
    ema50 = df_h1['close'].ewm(span=50, adjust=False).mean().iloc[-2]
    h1_c, c_prev = df_h1['close'].iloc[-2], df_m15.iloc[-2]
    sig, en, sl, tp = None, 0.0, 0.0, 0.0

    if h1_c > ema50 and c_prev['close'] > daily_open:
        if c_prev['low'] < df_m15['low'].iloc[-7:-2].min() and c_prev['close'] > c_prev['open']:
            en = c_prev['high'] - (0.5 * (c_prev['high'] - c_prev['low']))
            sl = c_prev['low'] * (1.0 - SL_BUFFER_PERCENT)
            tp = en + (en - sl)
            sig = 'BUY'

    elif h1_c < ema50 and c_prev['close'] < daily_open:
        if c_prev['high'] > df_m15['high'].iloc[-7:-2].max() and c_prev['close'] < c_prev['open']:
            en = c_prev['low'] + (0.5 * (c_prev['high'] - c_prev['low']))
            sl = c_prev['high'] * (1.0 + SL_BUFFER_PERCENT)
            tp = en - (sl - en)
            sig = 'SELL'

    return sig, en, sl, tp


def update_orders_and_positions():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()

    rows = c.execute(
        "SELECT id, exchange_id, symbol, side, entry_price, sl_price, "
        "tp_price, order_id, bars_count, status "
        "FROM trades WHERE status IN ('PENDING', 'OPEN')"
    ).fetchall()

    for t_id, ex_id, sym, side, en, sl, tp, ord_id, bars, status in rows:
        if ex_id not in exchanges:
            continue

        try:
            curr_price = exchanges[ex_id].fetch_ticker(sym)['last']
            n_bars, is_sim = bars + 1, str(ord_id).startswith('sim_')
            tag = " (DRY RUN)" if is_sim else ""

            if status == 'PENDING':
                n_stat = 'PENDING'

                if (side == 'BUY' and curr_price <= en) or (side == 'SELL' and curr_price >= en):
                    n_stat = 'OPEN'
                    send_telegram(f"🔔 <b>ORDER FILLED{tag}</b> {sym} di {ex_id.upper()}!")

                elif side == 'BUY':
                    if curr_price >= tp:
                        n_stat = 'MISSED (TP First)'
                    elif curr_price <= sl:
                        n_stat = 'CANCELLED (SL First)'
                    elif n_bars > 30:
                        n_stat = 'INVALID (>30 Bars)'

                elif side == 'SELL':
                    if curr_price <= tp:
                        n_stat = 'MISSED (TP First)'
                    elif curr_price >= sl:
                        n_stat = 'CANCELLED (SL First)'
                    elif n_bars > 30:
                        n_stat = 'INVALID (>30 Bars)'

                if n_stat != 'PENDING':
                    if ('CANCEL' in n_stat or 'MISS' in n_stat or 'INV' in n_stat) and not is_sim:
                        try:
                            exchanges[ex_id].cancel_order(ord_id, sym)
                        except Exception:
                            pass

                    send_telegram(f"⚪ <b>AUTO-CANCEL{tag}</b> {sym}\nAlasan: {n_stat}")
                    c.execute(
                        "UPDATE trades SET status=?, bars_count=? WHERE id=?",
                        (n_stat, n_bars, t_id)
                    )
                else:
                    c.execute(
                        "UPDATE trades SET bars_count=? WHERE id=?",
                        (n_bars, t_id)
                    )

            elif status == 'OPEN':
                if (side == 'BUY' and curr_price >= tp) or (side == 'SELL' and curr_price <= tp):
                    c.execute(
                        "UPDATE trades SET status='CLOSED (TP)' WHERE id=?",
                        (t_id,)
                    )
                    send_telegram(f"🎉 <b>TAKE PROFIT{tag}</b> {sym} | +1R 🚀")

                elif (side == 'BUY' and curr_price <= sl) or (side == 'SELL' and curr_price >= sl):
                    c.execute(
                        "UPDATE trades SET status='CLOSED (SL)' WHERE id=?",
                        (t_id,)
                    )
                    send_telegram(f"❌ <b>STOP LOSS{tag}</b> {sym} | -1R 🔻")

        except Exception:
            pass

    conn.commit()
    conn.close()


# =================================================================
# MARKET SCANNER
# =================================================================
# LOGIC TRADE TIDAK DIUBAH
def scan_markets():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    c = conn.cursor()

    act_count = c.execute(
        "SELECT COUNT(*) FROM trades WHERE status IN ('PENDING', 'OPEN')"
    ).fetchone()[0]

    pair_perf = {}
    for sym, stat in c.execute(
        "SELECT symbol, status FROM trades "
        "WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)')"
    ).fetchall():
        if sym not in pair_perf:
            pair_perf[sym] = {'w': 0, 'l': 0}
        if 'TP' in stat:
            pair_perf[sym]['w'] += 1
        else:
            pair_perf[sym]['l'] += 1

    conn.close()

    for ex_id, ex in exchanges.items():
        try:
            for sym in [s for s in ex.fetch_tickers().keys()
                        if s.endswith('/USDT:USDT') or s.endswith('/USDT')][:60]:

                if is_manual_alert_symbol(sym):
                    m15 = ex.fetch_ohlcv(sym, '15m', limit=50)
                    h1 = ex.fetch_ohlcv(sym, '1h', limit=60)
                    d1 = ex.fetch_ohlcv(sym, '1d', limit=2)

                    if m15 and h1 and d1:
                        sig, en, sl, tp = analyze_smc_retrace(
                            pd.DataFrame(m15, columns=['time','open','high','low','close','vol']),
                            pd.DataFrame(h1, columns=['time','open','high','low','close','vol']),
                            d1[-1][1]
                        )
                        if sig:
                            send_telegram(f"📢 <b>FX ALERT</b> {sym} | {sig}")
                    continue

                if act_count >= CONFIG['MAX_CONCURRENT_POSITIONS']:
                    continue

                if sym in pair_perf and sym not in CORE_PAIRS:
                    tot_trades = pair_perf[sym]['w'] + pair_perf[sym]['l']
                    if tot_trades >= CONFIG['MIN_TRADES']:
                        wr = (pair_perf[sym]['w'] / tot_trades) * 100
                        if wr < CONFIG['MIN_WR']:
                            continue

                m15 = ex.fetch_ohlcv(sym, '15m', limit=50)
                h1 = ex.fetch_ohlcv(sym, '1h', limit=60)
                d1 = ex.fetch_ohlcv(sym, '1d', limit=2)

                if not m15 or not h1 or not d1:
                    continue

                sig, en, sl, tp = analyze_smc_retrace(
                    pd.DataFrame(m15, columns=['time','open','high','low','close','vol']),
                    pd.DataFrame(h1, columns=['time','open','high','low','close','vol']),
                    d1[-1][1]
                )

                if sig:
                    lev = CONFIG['LEVERAGE'].get(ex_id, 50)
                    qty = (CONFIG['MARGIN_PER_TRADE'] * lev) / en
                    mode = "[DRY RUN]" if CONFIG['DRY_RUN'] else "[REAL TRADE]"
                    ord_id = f"sim_{int(time.time()*1000)}"

                    if not CONFIG['DRY_RUN']:
                        try:
                            ex.set_leverage(lev, sym)
                        except Exception:
                            pass

                        try:
                            ord_id = str(ex.create_order(
                                sym,
                                'limit',
                                'buy' if sig == 'BUY' else 'sell',
                                qty,
                                en,
                                params={
                                    'stopLoss': {'price': sl},
                                    'takeProfit': {'price': tp}
                                }
                            )['id'])
                        except Exception as e:
                            send_error_alert(f"Order Fails {sym}: {e}")
                            continue

                    conn = sqlite3.connect(DB_FILE, timeout=10)
                    conn.cursor().execute(
                        "INSERT INTO trades "
                        "(exchange_id, symbol, order_type, side, entry_price, "
                        "sl_price, tp_price, order_id, status) "
                        "VALUES (?, ?, 'LIMIT', ?, ?, ?, ?, ?, 'PENDING')",
                        (ex_id, sym, sig, en, sl, tp, ord_id)
                    )
                    conn.commit()
                    conn.close()

                    send_telegram(
                        f"🎯 <b>{mode}</b>\n"
                        f"CEX: {ex_id.upper()} | {sym} | {sig}\n"
                        f"Entry: {en:.5f}"
                    )
                    act_count += 1

        except Exception as e:
            send_error_alert(f"Scan Ex [{ex_id}]: {e}")


# =================================================================
# TELEGRAM BOT - AIOGRAM + INLINE KEYBOARD + CALLBACK QUERY
# =================================================================
bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# Conversational API setup state.
# {chat_id: {stage, ex_id, api, prompt_message_id, ack_message_id}}
# Prompt/ack message IDs are tracked so the API setup conversation can be cleaned up.
PENDING_API_SETUP = {}


def is_allowed_chat(message_or_callback):
    chat = message_or_callback.chat if hasattr(message_or_callback, 'chat') else message_or_callback.message.chat
    return str(chat.id) == str(TELEGRAM_CHAT_ID)


def main_keyboard():
    # Semua fitur yang memang sudah ada di script dipusatkan di inline keyboard.
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 POSISI", callback_data="positions"),
         InlineKeyboardButton(text="📈 STATISTIK", callback_data="stats")],
        [InlineKeyboardButton(text="⚙️ SETTINGS", callback_data="settings"),
         InlineKeyboardButton(text="🏆 PAIR PERF", callback_data="pairperf")],
        [InlineKeyboardButton(text="🏦 CEX", callback_data="cex_status"),
         InlineKeyboardButton(text="🔐 API", callback_data="api_menu")],
        [InlineKeyboardButton(text="⏯ BOT CONTROL", callback_data="bot_control")],
        [InlineKeyboardButton(text="🧹 CANCEL ALL", callback_data="cancelall")],
        [InlineKeyboardButton(text="🔄 REFRESH", callback_data="menu")]
    ])


def bot_control_keyboard():
    dry = "🧪 DRY RUN 🟢" if CONFIG['DRY_RUN'] else "🧪 DRY RUN 🔴"
    daily = "🛑 DAILY LOSS 🟢" if DAILY_LOSS_ENABLED else "🛑 DAILY LOSS 🔴"
    pause = "▶️ RESUME" if IS_BOT_PAUSED else "⏸ PAUSE"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=dry, callback_data="dryrun_toggle")],
        [InlineKeyboardButton(text=daily, callback_data="daily_loss_toggle")],
        [InlineKeyboardButton(text=pause, callback_data="resume" if IS_BOT_PAUSED else "pause")],
        [InlineKeyboardButton(text="🧹 CANCEL ALL", callback_data="cancelall")],
        [InlineKeyboardButton(text="⬅️ Kembali", callback_data="menu")]
    ])


def settings_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 MARGIN", callback_data="set_margin_menu"),
         InlineKeyboardButton(text="📊 MAX POS", callback_data="set_maxpos_menu")],
        [InlineKeyboardButton(text="🎯 MIN WR", callback_data="set_minwr_menu"),
         InlineKeyboardButton(text="📈 MIN TRADES", callback_data="set_mintrades_menu")],
        [InlineKeyboardButton(text="⚡ LEVERAGE", callback_data="set_lev_menu")],
        [InlineKeyboardButton(text="⬅️ Kembali", callback_data="menu")]
    ])


def preset_keyboard(title, values, prefix, back="settings"):
    rows = []
    row = []
    for value, label in values:
        row.append(InlineKeyboardButton(text=label, callback_data=f"{prefix}:{value}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Kembali", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cex_api_keyboard():
    rows = []
    for ex_id in DEFAULT_CEX:
        rows.append([InlineKeyboardButton(text=f"🔑 {ex_id.upper()}", callback_data=f"api_setup:{ex_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Kembali", callback_data="api_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cex_control_keyboard():
    rows = []
    db_rows = db_read("SELECT ex_id, enabled, api_key, account_label FROM cex_settings")
    for ex_id, enabled, api_key, account_label in db_rows:
        state = "🟢 ON" if enabled else "🔴 OFF"
        toggle_action = "off" if enabled else "on"
        rows.append([
            InlineKeyboardButton(text=f"{ex_id.upper()} {state}", callback_data=f"cex_toggle:{ex_id}:{toggle_action}"),
            InlineKeyboardButton(text="🔑 API", callback_data=f"api_setup:{ex_id}")
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Kembali", callback_data="api_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def api_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Set / Ganti API", callback_data="api_setup_menu")],
        [InlineKeyboardButton(text="🏦 ON / OFF CEX", callback_data="cex_control")],
        [InlineKeyboardButton(text="📋 Lihat API & Akun", callback_data="api_status")],
        [InlineKeyboardButton(text="⬅️ Kembali", callback_data="menu")]
    ])


def leverage_keyboard():
    rows = []
    for ex_id in DEFAULT_CEX:
        rows.append([InlineKeyboardButton(
            text=f"⚡ {ex_id.upper()} ({CONFIG['LEVERAGE'].get(ex_id, 50)}x)",
            callback_data=f"lev_ex:{ex_id}"
        )])
    rows.append([InlineKeyboardButton(text="⚡ SEMUA CEX", callback_data="lev_all")])
    rows.append([InlineKeyboardButton(text="⬅️ Kembali", callback_data="settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def leverage_value_keyboard(target):
    values = [(5,'5x'), (10,'10x'), (20,'20x'), (25,'25x'), (50,'50x'), (75,'75x'), (100,'100x')]
    rows = []
    row = []
    for value, label in values:
        row.append(InlineKeyboardButton(text=label, callback_data=f"lev_set:{target}:{value}"))
        if len(row) == 3:
            rows.append(row); row=[]
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Kembali", callback_data="set_lev_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def db_read(query, params=()):
    conn = sqlite3.connect(DB_FILE, timeout=5)
    try:
        return conn.cursor().execute(query, params).fetchall()
    finally:
        conn.close()


def build_settings_text():
    cex_db = db_read("SELECT ex_id, api_key, enabled, account_label, account_info FROM cex_settings")
    mode_text = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL MONEY"
    lines = []
    for ex_id, api, en, account_label, account_info in cex_db:
        loaded = ex_id in exchanges
        account_text = html.escape(account_label) if account_label else 'Belum diverifikasi'
        lines.append(
            f"• {ex_id.upper()}: {'🟢 ON' if en else '🔴 OFF'} | "
            f"Lev: {CONFIG['LEVERAGE'].get(ex_id, 50)}x | "
            f"API: {'✅' if api else '❌'} | "
            f"Conn: {'🟢' if loaded else '⚪'}\n"
            f"  👤 Akun: {account_text}"
        )
    return (
        "⚙️ <b>SETTING BOT</b>\n\n"
        f"Mode: {mode_text}\n"
        f"Margin: ${CONFIG['MARGIN_PER_TRADE']}\n"
        f"Max Pos: {CONFIG['MAX_CONCURRENT_POSITIONS']}\n"
        f"Min Trade: {CONFIG['MIN_TRADES']}\n"
        f"Min WR: {CONFIG['MIN_WR']}%\n\n"
        f"<b>BURSA:</b>\n{chr(10).join(lines) if lines else 'Belum ada'}"
    )


def build_positions_text():
    rows = db_read(
        "SELECT exchange_id, symbol, side, entry_price, sl_price, tp_price, status "
        "FROM trades WHERE status IN ('PENDING', 'OPEN') ORDER BY id DESC"
    )
    cex_db = db_read("SELECT ex_id, enabled FROM cex_settings")
    cex_lines = [f"• {ex.upper()} {'🟢 ON' if en else '🔴 OFF'}" for ex, en in cex_db]
    mode = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL"
    text = (
        "📊 <b>STATUS & POSISI</b>\n"
        f"Bot: {'⏸ PAUSED' if IS_BOT_PAUSED else '🟢 RUNNING'} | Mode: {mode}\n\n"
        "🔌 <b>CEX:</b>\n" + ("\n".join(cex_lines) if cex_lines else "• Tidak ada CEX") +
        f"\n\n📋 <b>ANTREAN:</b> {len(rows)}\n"
    )
    if not rows:
        text += "\n🟢 Tidak ada posisi pending/open."
    else:
        for ex_id, sym, side, en, sl, tp, status in rows:
            text += (
                f"\n• <b>{ex_id.upper()} | {sym}</b> | {side}\n"
                f"  Status: {status}\n"
                f"  Entry: {en:.8f}\n"
                f"  SL: {sl:.8f}\n"
                f"  TP: {tp:.8f}\n"
            )
    return text


def build_stats_text():
    rows = db_read("SELECT status, COUNT(*) FROM trades GROUP BY status")
    wins = sum(n for status, n in rows if status == 'CLOSED (TP)')
    losses = sum(n for status, n in rows if status == 'CLOSED (SL)')
    total = wins + losses
    wr = (wins / total * 100) if total else 0
    net = (wins - losses) * CONFIG.get('MARGIN_PER_TRADE', 2.0)
    detail = "\n".join(f"• {status}: {n}" for status, n in rows) or "Belum ada data"
    return (
        "📈 <b>STATISTIK</b>\n\n"
        f"Trades Closed: {total}\n"
        f"Win: {wins}\n"
        f"Loss: {losses}\n"
        f"Win Rate: {wr:.1f}%\n"
        f"Net PnL (estimasi): ${net:.2f}\n\n"
        f"{detail}"
    )


def build_pairperf_text():
    rows = db_read(
        "SELECT symbol, status FROM trades WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)')"
    )
    pair_perf = {}
    for sym, stat in rows:
        pair_perf.setdefault(sym, {'w': 0, 'l': 0})
        if 'TP' in stat:
            pair_perf[sym]['w'] += 1
        else:
            pair_perf[sym]['l'] += 1

    active, wait, black = [], [], []
    for sym, data in pair_perf.items():
        total = data['w'] + data['l']
        wr = data['w'] / total * 100 if total else 0
        core = sym in CORE_PAIRS
        line = f"• {sym}{' [CORE]' if core else ''}: {data['w']}W/{data['l']}L (WR: {wr:.1f}%)"
        if total < CONFIG['MIN_TRADES']:
            wait.append(line)
        elif core or wr >= CONFIG['MIN_WR']:
            active.append(line)
        else:
            black.append(line)

    return (
        "🏆 <b>PERFORMA KOIN</b>\n\n"
        "🔥 <b>AKTIF</b>\n" + ("\n".join(active) or "Belum ada") + "\n\n"
        "⏳ <b>EVALUASI</b>\n" + ("\n".join(wait) or "Belum ada") + "\n\n"
        "🗑️ <b>BLACKLIST</b>\n" + ("\n".join(black) or "Belum ada")
    )


def build_cex_status_text():
    rows = db_read("SELECT ex_id, api_key, enabled, account_label, account_info FROM cex_settings")
    if not rows:
        return "🏦 <b>CEX STATUS</b>\nBelum ada CEX."
    lines = []
    for ex_id, api, enabled, account_label, account_info in rows:
        if not enabled:
            state = "🔴 OFF"
        elif ex_id in exchanges:
            state = "🟢 LOADED"
        else:
            state = "🟡 CONFIGURED / NOT LOADED"
        account = html.escape(account_label) if account_label else 'Belum diverifikasi'
        lines.append(
            f"🏦 <b>{ex_id.upper()}</b> — {state}\n"
            f"   🔑 API: {'✅' if api else '❌'} | Lev: {CONFIG['LEVERAGE'].get(ex_id, 50)}x\n"
            f"   👤 {account}"
        )
        if account_info:
            try:
                info=json.loads(account_info)
                extras=[]
                for k in ('USDT Total','USDT Free','Open Positions','Open Orders','Private API'):
                    if k in info: extras.append(f"{k}: {info[k]}")
                if extras: lines.append('   ' + ' | '.join(extras))
            except Exception:
                pass
    return "🏦 <b>CEX STATUS</b>\n\n" + "\n\n".join(lines)


def _deep_find_values(obj, wanted_keys):
    found = {}
    wanted = {str(k).lower() for k in wanted_keys}

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                kl = str(k).lower()
                if kl in wanted and v not in (None, '') and kl not in found:
                    found[kl] = str(v)
                if len(found) < len(wanted):
                    walk(v)
        elif isinstance(x, list):
            for v in x:
                if len(found) < len(wanted):
                    walk(v)

    walk(obj)
    return found


def _safe_call(fn):
    try:
        return fn()
    except Exception:
        return None


def extract_account_details(ex, balance):
    info = balance.get('info', {}) if isinstance(balance, dict) else {}
    values = _deep_find_values(info, [
        'username', 'userName', 'user_name', 'nickname', 'nickName',
        'displayName', 'display_name', 'accountName', 'account_name',
        'uid', 'userId', 'user_id', 'memberId', 'member_id',
        'accountId', 'account_id', 'accountAlias', 'subAccountId',
        'subAccountName', 'accountType', 'email', 'marginMode',
        'unifiedMarginStatus', 'tradePermission', 'readOnly'
    ])

    def pick(*keys):
        for key in keys:
            value = values.get(key.lower())
            if value:
                return value
        return '-'

    usdt = balance.get('USDT', {}) if isinstance(balance, dict) else {}
    free = total = used = None
    if isinstance(usdt, dict):
        for name, target in [('free','free'), ('total','total'), ('used','used')]:
            try:
                val = usdt.get(name)
                if val is not None:
                    if target == 'free': free = float(val)
                    elif target == 'total': total = float(val)
                    else: used = float(val)
            except Exception:
                pass

    details = {
        'username': pick('username', 'userName', 'user_name', 'nickname', 'nickName', 'displayName', 'display_name', 'accountName', 'account_name'),
        'uid': pick('uid', 'userId', 'user_id', 'memberId', 'member_id', 'accountId', 'account_id'),
        'account_type': pick('accountType'),
        'alias': pick('accountAlias', 'subAccountName', 'subAccountId'),
        'free_usdt': free,
        'total_usdt': total,
        'used_usdt': used,
        'margin_mode': pick('marginMode'),
        'read_only': pick('readOnly'),
        'trade_permission': pick('tradePermission')
    }
    return details


def enrich_account_details(ex, details):
    # These calls only READ account data. They never place an order.
    positions = _safe_call(lambda: ex.fetch_positions())
    if isinstance(positions, list):
        details['open_positions'] = sum(1 for p in positions if isinstance(p, dict) and (p.get('contracts') or p.get('contractSize') or p.get('info')))
    else:
        details['open_positions'] = 'N/A'

    orders = _safe_call(lambda: ex.fetch_open_orders())
    if isinstance(orders, list):
        details['open_orders'] = len(orders)
    else:
        details['open_orders'] = 'N/A'

    details['private_api'] = 'READ OK'
    details['exchange'] = ex.id.upper()
    details['market_type'] = str(ex.options.get('defaultType', '-')).upper()
    return details


def format_account_details(details):
    def money(v):
        return f"${v:,.4f}" if isinstance(v, (int, float)) else '-'
    def val(key):
        v = details.get(key, '-')
        return html.escape(str(v)) if v not in (None, '') else '-'
    return (
        f"🏦 <b>Exchange:</b> {val('exchange')}\n"
        f"👤 <b>Username/Nama:</b> {val('username')}\n"
        f"🆔 <b>UID/Account ID:</b> {val('uid')}\n"
        f"🏷️ <b>Alias:</b> {val('alias')}\n"
        f"📁 <b>Account Type:</b> {val('account_type')}\n"
        f"📐 <b>Market:</b> {val('market_type')}\n"
        f"🔒 <b>Private API:</b> {val('private_api')}\n"
        f"💵 <b>USDT Total:</b> {money(details.get('total_usdt'))}\n"
        f"🟢 <b>USDT Free:</b> {money(details.get('free_usdt'))}\n"
        f"🟡 <b>USDT Used:</b> {money(details.get('used_usdt'))}\n"
        f"📊 <b>Open Positions:</b> {val('open_positions')}\n"
        f"📋 <b>Open Orders:</b> {val('open_orders')}"
    )


def build_api_status_text():
    rows = db_read("SELECT ex_id, api_key, enabled, account_label, account_info FROM cex_settings")
    if not rows:
        return "🔐 <b>API & AKUN</b>\n\nBelum ada konfigurasi CEX."
    blocks = []
    for ex_id, api, enabled, account_label, account_info in rows:
        state = "🟢 ON" if enabled else "🔴 OFF"
        lines = [
            f"🏦 <b>{ex_id.upper()}</b> — {state}",
            f"🔑 API: {'✅ Tersimpan' if api else '❌ Belum ada'}",
            f"👤 Akun: {html.escape(account_label) if account_label else 'Belum diverifikasi'}"
        ]
        if ex_id == 'bitget':
            pw = db_read("SELECT password FROM cex_settings WHERE ex_id=?", (ex_id,))
            has_passphrase = bool(pw and pw[0][0])
            lines.insert(2, f"🔐 Passphrase: {'✅ Tersimpan' if has_passphrase else '❌ Belum ada'}")
        if account_info:
            try:
                info = json.loads(account_info)
                order = [
                    ('Exchange','🏦'), ('Account Type','📁'), ('Market','📐'),
                    ('Private API','🔒'), ('USDT Total','💵'), ('USDT Free','🟢'),
                    ('USDT Used','🟡'), ('Open Positions','📊'), ('Open Orders','📋'),
                    ('Trade Permission','🛡️'), ('Read Only','👁️')
                ]
                for key, icon in order:
                    if key in info:
                        value = html.escape(str(info[key]))
                        lines.append(f"{icon} {key}: {value}")
            except Exception:
                pass
        blocks.append("\n".join(lines))
    return "🔐 <b>API & INFORMASI AKUN</b>\n\n" + "\n\n".join(blocks)


def validate_cex_api(ex_id, api, secret, password=''):
    """Validate credentials without placing an order.

    Bitget calls its third API credential an API Passphrase, while CCXT
    exposes that credential through the unified `password` field.
    """
    api = (api or '').strip()
    secret = (secret or '').strip()
    password = (password or '').strip()

    if ex_id not in DEFAULT_CEX:
        return False, "CEX tidak dikenal.", None

    if not api or not secret:
        return False, "API key/secret kosong.", None

    if ex_id == 'bitget' and not password:
        return False, "Bitget membutuhkan API Passphrase.", None

    try:
        params = {
            'apiKey': api,
            'secret': secret,
            'options': {
                'defaultType': DEFAULT_CEX[ex_id]['type']
            },
            'enableRateLimit': True,
            'timeout': 10000
        }

        # IMPORTANT:
        # CCXT maps Bitget's API Passphrase to `password`.
        if ex_id == 'bitget':
            params['password'] = password

        ex = getattr(ccxt, ex_id)(params)

        # Fail early with a clear local error if CCXT still sees a
        # missing credential instead of producing a vague API error.
        if hasattr(ex, 'check_required_credentials'):
            ex.check_required_credentials()

        balance = ex.fetch_balance()
        details = extract_account_details(ex, balance)
        details = enrich_account_details(ex, details)

        return True, (
            "API valid dan private endpoint berhasil diakses."
        ), details

    except Exception as e:
        return False, str(e)[:1200], None


async def safe_answer(callback: CallbackQuery, text=None, show_alert=False):
    try:
        await callback.answer(text or "OK", show_alert=show_alert)
    except Exception:
        pass


@dp.message(Command("start"))
@dp.message(Command("help"))
async def cmd_start(message: Message):
    if not is_allowed_chat(message):
        return
    await message.answer(
        "🤖 <b>ALCHEMIST v2</b>\n\nPilih menu:",
        reply_markup=main_keyboard()
    )


@dp.message(Command("settings", "setting"))
async def cmd_settings(message: Message):
    if not is_allowed_chat(message):
        return
    await message.answer(build_settings_text(), reply_markup=main_keyboard())


@dp.message(Command("posisi"))
async def cmd_posisi(message: Message):
    if not is_allowed_chat(message):
        return
    await message.answer(build_positions_text(), reply_markup=main_keyboard())


@dp.message(Command("stat"))
async def cmd_stat(message: Message):
    if not is_allowed_chat(message):
        return
    await message.answer(build_stats_text(), reply_markup=main_keyboard())


@dp.message(Command("pairperf"))
async def cmd_pairperf(message: Message):
    if not is_allowed_chat(message):
        return
    await message.answer(build_pairperf_text(), reply_markup=main_keyboard())


@dp.message(Command("pause"))
async def cmd_pause(message: Message):
    global IS_BOT_PAUSED
    if not is_allowed_chat(message):
        return
    IS_BOT_PAUSED = True
    await message.answer("⏸️ <b>Bot PAUSED</b>", reply_markup=main_keyboard())


@dp.message(Command("resume"))
async def cmd_resume(message: Message):
    global IS_BOT_PAUSED
    if not is_allowed_chat(message):
        return
    IS_BOT_PAUSED = False
    await message.answer("▶️ <b>Bot RESUMED</b>", reply_markup=main_keyboard())


@dp.message(Command("cancelall"))
async def cmd_cancelall(message: Message):
    if not is_allowed_chat(message):
        return
    cancel_all_pending_orders_on_pause()
    await message.answer("🧹 Semua pending order diproses untuk dibatalkan.", reply_markup=main_keyboard())


@dp.message(Command("dryrun"))
async def cmd_dryrun(message: Message):
    if not is_allowed_chat(message):
        return
    parts = (message.text or '').split()
    if len(parts) != 2 or parts[1].lower() not in ('on', 'off'):
        await message.answer("⚠️ Format: /dryrun on atau /dryrun off")
        return
    value = parts[1].lower() == 'on'
    CONFIG['DRY_RUN'] = value
    update_db_setting('dry_run', 1.0 if value else 0.0)
    await message.answer("✅ DRY RUN AKTIF" if value else "⚠️ REAL MONEY AKTIF", reply_markup=main_keyboard())


@dp.message(Command("setmargin"))
async def cmd_setmargin(message: Message):
    if not is_allowed_chat(message): return
    try:
        value = float((message.text or '').split()[1])
        CONFIG['MARGIN_PER_TRADE'] = value
        update_db_setting('margin', value)
        await message.answer(f"✅ Margin: ${value}", reply_markup=main_keyboard())
    except Exception:
        await message.answer("⚠️ Format: /setmargin 2")


@dp.message(Command("setmaxpos"))
async def cmd_setmaxpos(message: Message):
    if not is_allowed_chat(message): return
    try:
        value = int((message.text or '').split()[1])
        CONFIG['MAX_CONCURRENT_POSITIONS'] = value
        update_db_setting('max_pos', value)
        await message.answer(f"✅ Max Posisi: {value}", reply_markup=main_keyboard())
    except Exception:
        await message.answer("⚠️ Format: /setmaxpos 2")


@dp.message(Command("setminwr"))
async def cmd_setminwr(message: Message):
    if not is_allowed_chat(message): return
    try:
        value = float((message.text or '').split()[1])
        CONFIG['MIN_WR'] = value
        update_db_setting('min_wr', value)
        await message.answer(f"✅ Min WinRate: {value}%", reply_markup=main_keyboard())
    except Exception:
        await message.answer("⚠️ Format: /setminwr 40")


@dp.message(Command("setmintrades"))
async def cmd_setmintrades(message: Message):
    if not is_allowed_chat(message): return
    try:
        value = int((message.text or '').split()[1])
        CONFIG['MIN_TRADES'] = value
        update_db_setting('min_trades', value)
        await message.answer(f"✅ Min Trade: {value}x", reply_markup=main_keyboard())
    except Exception:
        await message.answer("⚠️ Format: /setmintrades 5")


@dp.message(Command("setlev"))
async def cmd_setlev(message: Message):
    if not is_allowed_chat(message): return
    try:
        parts = (message.text or '').split()
        if len(parts) == 2:
            value = int(parts[1])
            for ex_id in DEFAULT_CEX:
                CONFIG['LEVERAGE'][ex_id] = value
                update_db_setting(f'lev_{ex_id}', value)
            await message.answer(f"✅ Semua leverage: {value}x", reply_markup=main_keyboard())
        elif len(parts) == 3:
            ex_id, value = parts[1].lower(), int(parts[2])
            if ex_id not in DEFAULT_CEX:
                raise ValueError("CEX tidak dikenal")
            CONFIG['LEVERAGE'][ex_id] = value
            update_db_setting(f'lev_{ex_id}', value)
            await message.answer(f"✅ Lev {ex_id.upper()}: {value}x", reply_markup=main_keyboard())
        else:
            raise ValueError
    except Exception:
        await message.answer("⚠️ Format: /setlev 50 atau /setlev bybit 50")


@dp.message(Command("cex"))
async def cmd_cex(message: Message):
    if not is_allowed_chat(message): return
    try:
        parts = (message.text or '').split()
        action, ex_id = parts[1].lower(), parts[2].lower()
        if ex_id not in DEFAULT_CEX or action not in ('on', 'off'):
            raise ValueError
        conn = sqlite3.connect(DB_FILE, timeout=5)
        conn.cursor().execute("UPDATE cex_settings SET enabled=? WHERE ex_id=?", (1 if action == 'on' else 0, ex_id))
        conn.commit(); conn.close()
        reload_exchanges()
        await message.answer(f"✅ CEX {ex_id.upper()} di-{action.upper()}!", reply_markup=main_keyboard())
    except Exception:
        await message.answer("⚠️ Format: /cex on/off <cex>")


@dp.message(Command("setapi"))
async def cmd_setapi(message: Message):
    if not is_allowed_chat(message):
        return
    await message.answer(
        "🔐 <b>SET API</b>\n\nPilih CEX dulu. Bot akan meminta <b>API Key</b> lalu <b>Secret Key</b>. Untuk <b>BITGET</b>, bot juga meminta <b>API Passphrase</b> (bukan password login).",
        reply_markup=cex_api_keyboard()
    )


@dp.callback_query(F.data == "api_menu")
async def cb_api_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(
        "🔐 <b>API MANAGEMENT</b>\n\nPilih tindakan:",
        reply_markup=api_menu_keyboard()
    )


@dp.callback_query(F.data == "api_setup_menu")
async def cb_api_setup_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(
        "🔑 <b>PILIH CEX</b>\n\nBot akan meminta API Key dan Secret Key satu per satu. <b>BITGET</b> juga membutuhkan API Passphrase.",
        reply_markup=cex_api_keyboard()
    )


@dp.callback_query(F.data.startswith("api_setup:"))
async def cb_api_setup(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    ex_id = callback.data.split(":", 1)[1]
    if ex_id not in DEFAULT_CEX:
        await safe_answer(callback, "CEX tidak dikenal", True)
        return
    PENDING_API_SETUP[callback.message.chat.id] = {
        'stage': 'api_key',
        'ex_id': ex_id,
        'prompt_message_id': None,
        'ack_message_id': None
    }
    await safe_answer(callback, f"Pilih {ex_id.upper()}")
    await callback.message.edit_text(
        (
            f"🔑 <b>SET API — {ex_id.upper()}</b>\n\n"
            "Kirim <b>API Key</b> saja di pesan berikutnya.\n"
            "Bot akan langsung meminta Secret Key setelahnya.\n"
            + ("Untuk BITGET, setelah Secret Key bot akan meminta <b>API Passphrase</b>.\n" if ex_id == 'bitget' else "")
            + "\n⚠️ Jangan kirim credential ke chat lain."
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Batal", callback_data="api_cancel")]
        ])
    )
    PENDING_API_SETUP[callback.message.chat.id]['prompt_message_id'] = callback.message.message_id


@dp.callback_query(F.data == "api_cancel")
async def cb_api_cancel(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    state = PENDING_API_SETUP.pop(callback.message.chat.id, None)
    await safe_answer(callback, "Dibatalkan")
    if state:
        for mid in (state.get('prompt_message_id'), state.get('ack_message_id')):
            if mid:
                try: await bot.delete_message(callback.message.chat.id, mid)
                except Exception: pass
    try:
        await callback.message.edit_text("🔐 <b>API MANAGEMENT</b>\n\nDibatalkan.", reply_markup=api_menu_keyboard())
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "cex_control")
async def cb_cex_control(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(
        "🏦 <b>CEX CONTROL</b>\n\nTekan tombol CEX untuk ON/OFF.\nGunakan 🔑 API untuk set/ganti API.",
        reply_markup=cex_control_keyboard()
    )


@dp.callback_query(F.data.startswith("cex_toggle:"))
async def cb_cex_toggle(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    _, ex_id, action = callback.data.split(":", 2)
    row = db_read("SELECT api_key, secret, password FROM cex_settings WHERE ex_id=?", (ex_id,))
    if not row:
        await safe_answer(callback, "CEX tidak ditemukan", True)
        return
    api, secret, password = row[0]
    if action == 'on' and (not api or not secret or (ex_id == 'bitget' and not password)):
        await safe_answer(callback, "Set API + Passphrase Bitget dulu sebelum ON" if ex_id == 'bitget' else "Set API dulu sebelum ON", True)
        return
    conn = sqlite3.connect(DB_FILE, timeout=5)
    conn.cursor().execute("UPDATE cex_settings SET enabled=? WHERE ex_id=?", (1 if action == 'on' else 0, ex_id))
    conn.commit(); conn.close()
    reload_exchanges()
    await safe_answer(callback, f"{ex_id.upper()} {action.upper()}")
    await callback.message.edit_text(
        "🏦 <b>CEX CONTROL</b>\n\nTekan tombol CEX untuk ON/OFF.\nGunakan 🔑 API untuk set/ganti API.",
        reply_markup=cex_control_keyboard()
    )


async def safe_delete_message(chat_id, message_id):
    """Best-effort deletion for credential/prompt messages."""
    if not message_id:
        return False
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except Exception:
        return False


@dp.message(F.text)
async def handle_api_setup_message(message: Message):
    if not is_allowed_chat(message):
        return

    chat_id = message.chat.id
    state = PENDING_API_SETUP.get(chat_id)
    if not state:
        return

    text = (message.text or "").strip()
    if not text:
        return

    ex_id = state.get("ex_id")

    # Delete the user's credential message immediately.
    # Telegram supports deleting user messages sent to the bot in private
    # chats within the Bot API's deletion window.
    await safe_delete_message(chat_id, message.message_id)

    # -------------------------------------------------------------
    # STEP 1: API KEY
    # -------------------------------------------------------------
    if state.get("stage") == "api_key":
        state["stage"] = "secret"
        state["api"] = text

        ack = await bot.send_message(
            chat_id,
            f"🔐 <b>API Key diterima</b> untuk {ex_id.upper()}\n"
            "🗑️ Pesan API Key sudah dihapus.\n\n"
            "Sekarang kirim <b>Secret Key</b> saja.\n"
            "Pesan Secret Key akan langsung dihapus juga.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="❌ BATAL & HAPUS SESI",
                    callback_data="api_cancel"
                )]
            ])
        )
        state["ack_message_id"] = ack.message_id
        return

    # -------------------------------------------------------------
    # STEP 2: SECRET KEY
    # -------------------------------------------------------------
    if state.get("stage") == "secret":
        state["secret"] = text

        if ex_id == "bitget":
            state["stage"] = "passphrase"

            ack = await bot.send_message(
                chat_id,
                "🔐 <b>Secret Key BITGET diterima</b>\n"
                "🗑️ Pesan Secret Key sudah dihapus.\n\n"
                "Sekarang kirim <b>API Passphrase</b> Bitget.\n"
                "⚠️ Ini adalah <b>Passphrase API</b> yang dibuat "
                "saat API Bitget dibuat, <b>bukan password login akun</b>.\n"
                "🗑️ Pesan Passphrase juga akan langsung dihapus.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text="❌ BATAL & HAPUS SESI",
                        callback_data="api_cancel"
                    )]
                ])
            )
            state["ack_message_id"] = ack.message_id
            return

        # Non-Bitget: secret is the final credential.
        api = state.get("api", "")
        secret = state.get("secret", "")
        password = ""

    # -------------------------------------------------------------
    # STEP 3: BITGET API PASSPHRASE
    # -------------------------------------------------------------
    elif state.get("stage") == "passphrase" and ex_id == "bitget":
        api = state.get("api", "")
        secret = state.get("secret", "")
        password = text

    else:
        return

    # Snapshot the complete credential set before clearing the session.
    credentials = (api, secret, password)

    PENDING_API_SETUP.pop(chat_id, None)

    # Remove the bot's previous prompts/acknowledgements too.
    for mid in (
        state.get("ack_message_id"),
        state.get("prompt_message_id")
    ):
        await safe_delete_message(chat_id, mid)

    status_msg = await bot.send_message(
        chat_id,
        f"🔄 <b>Memvalidasi API {ex_id.upper()}...</b>"
    )

    ok, detail, account_details = await asyncio.to_thread(
        validate_cex_api,
        ex_id,
        *credentials
    )

    if not ok:
        safe_detail = html.escape(str(detail))
        await status_msg.edit_text(
            f"❌ <b>API {ex_id.upper()} TIDAK VALID</b>\n\n"
            f"Alasan: <code>{safe_detail}</code>\n\n"
            "API <b>tidak disimpan</b>.\n"
            "Jika BITGET, pastikan yang dimasukkan adalah "
            "<b>API Passphrase</b>, bukan password login.",
            reply_markup=api_menu_keyboard()
        )
        return

    username = account_details.get("username", "-")
    uid = account_details.get("uid", "-")
    alias = account_details.get("alias", "-")

    label_parts = []
    if username != "-":
        label_parts.append(username)
    if uid != "-":
        label_parts.append(f"UID {uid}")
    if alias != "-":
        label_parts.append(alias)

    account_label = (
        " | ".join(label_parts)
        if label_parts
        else "Private account terverifikasi"
    )

    stored_info = {
        "Exchange": account_details.get("exchange", "-"),
        "Account Type": account_details.get("account_type", "-"),
        "Market": account_details.get("market_type", "-"),
        "Private API": account_details.get("private_api", "-"),
        "USDT Total": account_details.get("total_usdt"),
        "USDT Free": account_details.get("free_usdt"),
        "USDT Used": account_details.get("used_usdt"),
        "Open Positions": account_details.get("open_positions", "N/A"),
        "Open Orders": account_details.get("open_orders", "N/A"),
        "Trade Permission": account_details.get("trade_permission", "-"),
        "Read Only": account_details.get("read_only", "-")
    }
    stored_info = {
        k: v for k, v in stored_info.items()
        if v not in (None, "", "-")
    }

    saved_api, saved_secret, saved_password = credentials

    conn = sqlite3.connect(DB_FILE, timeout=5)
    conn.cursor().execute(
        "UPDATE cex_settings "
        "SET api_key=?, secret=?, password=?, account_label=?, account_info=? "
        "WHERE ex_id=?",
        (
            saved_api,
            saved_secret,
            saved_password if ex_id == "bitget" else "",
            account_label,
            json.dumps(stored_info, ensure_ascii=False),
            ex_id
        )
    )
    conn.commit()
    conn.close()

    # Clear all credential references immediately after persistence.
    saved_api = saved_secret = saved_password = ""
    credentials = ("", "", "")

    reload_exchanges()

    await status_msg.edit_text(
        f"✅ <b>API {ex_id.upper()} VALID</b>\n\n"
        "Private endpoint berhasil diakses.\n\n"
        f"{format_account_details(account_details)}\n\n"
        "🗑️ Pesan credential setup sudah dibersihkan.\n"
        "🔗 API sudah disimpan. CEX masih <b>OFF</b> "
        "sampai kamu menekan tombol ON.",
        reply_markup=cex_control_keyboard()
    )


@dp.callback_query(F.data == "menu")
async def cb_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    try:
        await callback.message.edit_text("🤖 <b>ALCHEMIST v2</b>\n\nPilih menu:", reply_markup=main_keyboard())
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "positions")
async def cb_positions(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(build_positions_text(), reply_markup=main_keyboard())


@dp.callback_query(F.data == "settings")
async def cb_settings(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(build_settings_text(), reply_markup=settings_keyboard())


@dp.callback_query(F.data == "stats")
async def cb_stats(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(build_stats_text(), reply_markup=main_keyboard())


@dp.callback_query(F.data == "pairperf")
async def cb_pairperf(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(build_pairperf_text(), reply_markup=main_keyboard())


@dp.callback_query(F.data == "cex_status")
async def cb_cex_status(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(build_cex_status_text(), reply_markup=main_keyboard())


@dp.callback_query(F.data == "api_status")
async def cb_api_status(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(build_api_status_text(), reply_markup=main_keyboard())


@dp.callback_query(F.data == "pause")
async def cb_pause(callback: CallbackQuery):
    global IS_BOT_PAUSED
    if not is_allowed_chat(callback): return
    IS_BOT_PAUSED = True
    notify_runtime_status("PAUSED", "⏸️ <b>TRADING PAUSED</b>\nPembukaan trade baru dihentikan. Monitoring posisi tetap berjalan.")
    await safe_answer(callback, "Bot di-pause")
    await callback.message.edit_text("⏸️ <b>Bot PAUSED</b>\nPembukaan trade baru dihentikan.", reply_markup=main_keyboard())


@dp.callback_query(F.data == "resume")
async def cb_resume(callback: CallbackQuery):
    global IS_BOT_PAUSED
    if not is_allowed_chat(callback): return
    IS_BOT_PAUSED = False
    notify_runtime_status("RESUMED", "▶️ <b>TRADING RESUMED</b>\nBot kembali boleh mencari setup baru.")
    await safe_answer(callback, "Bot dilanjutkan")
    await callback.message.edit_text("▶️ <b>Bot RESUMED</b>\nBot kembali aktif.", reply_markup=main_keyboard())


@dp.callback_query(F.data == "bot_control")
async def cb_bot_control(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text(
        "🤖 <b>BOT CONTROL</b>\n\n"
        f"Status: {'⏸ PAUSED' if IS_BOT_PAUSED else '🟢 RUNNING'}\n"
        f"Mode: {'🧪 DRY RUN' if CONFIG['DRY_RUN'] else '⚠️ REAL MONEY'}\n\n"
        "Gunakan tombol di bawah untuk mengubah kontrol yang memang tersedia di script.",
        reply_markup=bot_control_keyboard()
    )


@dp.callback_query(F.data == "dryrun_toggle")
async def cb_dryrun_toggle(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    value = not CONFIG['DRY_RUN']
    CONFIG['DRY_RUN'] = value
    update_db_setting('dry_run', 1.0 if value else 0.0)
    await safe_answer(callback, "DRY RUN ON" if value else "REAL MONEY ON")
    await callback.message.edit_text(
        f"{'🧪 <b>DRY RUN AKTIF</b>' if value else '⚠️ <b>REAL MONEY AKTIF</b>'}\n\n"
        "Mode berhasil diubah.",
        reply_markup=bot_control_keyboard()
    )
    send_telegram("🧪 <b>MODE BERUBAH</b> — " + ("DRY RUN aktif. Tidak ada order real." if value else "⚠️ REAL MONEY aktif. Order real diizinkan."))


@dp.callback_query(F.data == "daily_loss_toggle")
async def cb_daily_loss_toggle(callback: CallbackQuery):
    global DAILY_LOSS_ENABLED
    if not is_allowed_chat(callback):
        return
    DAILY_LOSS_ENABLED = not DAILY_LOSS_ENABLED
    update_db_setting('daily_loss_enabled', 1.0 if DAILY_LOSS_ENABLED else 0.0)
    await safe_answer(callback, "DAILY LOSS ON" if DAILY_LOSS_ENABLED else "DAILY LOSS OFF")
    await callback.message.edit_text(
        f"🛑 <b>DAILY LOSS LIMIT {'AKTIF' if DAILY_LOSS_ENABLED else 'NONAKTIF'}</b>\n\n"
        + (f"Batas: {MAX_DAILY_SL}x SL per hari.\nJika tercapai, pembukaan trade baru dipause." if DAILY_LOSS_ENABLED else "Proteksi daily loss dimatikan. Bot tidak akan auto-pause setelah batas SL harian tercapai."),
        reply_markup=bot_control_keyboard()
    )


@dp.callback_query(F.data == "cancelall")
async def cb_cancelall(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    cancel_all_pending_orders_on_pause()
    await safe_answer(callback, "Pending order diproses")
    await callback.message.edit_text(
        "🧹 <b>CANCEL ALL</b>\n\nSemua pending order yang terdaftar diproses untuk dibatalkan.",
        reply_markup=bot_control_keyboard()
    )


@dp.callback_query(F.data == "set_margin_menu")
async def cb_set_margin_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text("💵 <b>PILIH MARGIN / TRADE</b>", reply_markup=preset_keyboard("", [(1,'$1'),(2,'$2'),(5,'$5'),(10,'$10')], 'set_margin'))


@dp.callback_query(F.data.startswith("set_margin:"))
async def cb_set_margin(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    value=float(callback.data.split(':')[1])
    CONFIG['MARGIN_PER_TRADE']=value
    update_db_setting('margin', value)
    await safe_answer(callback, f"Margin ${value}")
    await callback.message.edit_text(f"✅ <b>Margin disetel:</b> ${value}", reply_markup=settings_keyboard())


@dp.callback_query(F.data == "set_maxpos_menu")
async def cb_set_maxpos_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text("📊 <b>PILIH MAX POSISI</b>", reply_markup=preset_keyboard("", [(1,'1'),(2,'2'),(3,'3'),(5,'5')], 'set_maxpos'))


@dp.callback_query(F.data.startswith("set_maxpos:"))
async def cb_set_maxpos(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    value=int(callback.data.split(':')[1])
    CONFIG['MAX_CONCURRENT_POSITIONS']=value
    update_db_setting('max_pos', value)
    await safe_answer(callback, f"Max posisi {value}")
    await callback.message.edit_text(f"✅ <b>Max posisi:</b> {value}", reply_markup=settings_keyboard())


@dp.callback_query(F.data == "set_minwr_menu")
async def cb_set_minwr_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text("🎯 <b>PILIH MINIMUM WIN RATE</b>", reply_markup=preset_keyboard("", [(30,'30%'),(40,'40%'),(50,'50%'),(60,'60%'),(70,'70%')], 'set_minwr'))


@dp.callback_query(F.data.startswith("set_minwr:"))
async def cb_set_minwr(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    value=float(callback.data.split(':')[1])
    CONFIG['MIN_WR']=value
    update_db_setting('min_wr', value)
    await safe_answer(callback, f"Min WR {value}%")
    await callback.message.edit_text(f"✅ <b>Min Win Rate:</b> {value}%", reply_markup=settings_keyboard())


@dp.callback_query(F.data == "set_mintrades_menu")
async def cb_set_mintrades_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text("📈 <b>PILIH MINIMUM TRADE</b>", reply_markup=preset_keyboard("", [(3,'3'),(5,'5'),(10,'10'),(20,'20')], 'set_mintrades'))


@dp.callback_query(F.data.startswith("set_mintrades:"))
async def cb_set_mintrades(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    value=int(callback.data.split(':')[1])
    CONFIG['MIN_TRADES']=value
    update_db_setting('min_trades', value)
    await safe_answer(callback, f"Min trades {value}")
    await callback.message.edit_text(f"✅ <b>Min Trades:</b> {value}", reply_markup=settings_keyboard())


@dp.callback_query(F.data == "set_lev_menu")
async def cb_set_lev_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text("⚡ <b>PILIH CEX UNTUK LEVERAGE</b>", reply_markup=leverage_keyboard())


@dp.callback_query(F.data == "lev_all")
async def cb_lev_all(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    await callback.message.edit_text("⚡ <b>LEVERAGE SEMUA CEX</b>", reply_markup=leverage_value_keyboard('all'))


@dp.callback_query(F.data.startswith("lev_ex:"))
async def cb_lev_ex(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    ex_id=callback.data.split(':',1)[1]
    await safe_answer(callback)
    await callback.message.edit_text(f"⚡ <b>LEVERAGE {ex_id.upper()}</b>\nSaat ini: {CONFIG['LEVERAGE'].get(ex_id,50)}x", reply_markup=leverage_value_keyboard(ex_id))


@dp.callback_query(F.data.startswith("lev_set:"))
async def cb_lev_set(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    _, target, value_s = callback.data.split(':',2)
    value=int(value_s)
    if target == 'all':
        for ex_id in DEFAULT_CEX:
            CONFIG['LEVERAGE'][ex_id]=value
            update_db_setting(f'lev_{ex_id}', value)
        text=f"✅ <b>Semua leverage:</b> {value}x"
    else:
        if target not in DEFAULT_CEX:
            await safe_answer(callback, "CEX tidak dikenal", True); return
        CONFIG['LEVERAGE'][target]=value
        update_db_setting(f'lev_{target}', value)
        text=f"✅ <b>Leverage {target.upper()}:</b> {value}x"
    await safe_answer(callback, "Leverage diperbarui")
    await callback.message.edit_text(text, reply_markup=settings_keyboard())


async def aiogram_polling():
    # IMPORTANT: Aiogram polling runs in the MAIN THREAD.
    # Python signal handlers cannot be installed from a worker thread.
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(
        bot,
        allowed_updates=dp.resolve_used_update_types()
    )


# =================================================================
# WEB DASHBOARD & FLASK SERVER
# =================================================================
app = Flask(__name__, template_folder='templates')

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/api/stats')
def api_stats():
    conn = sqlite3.connect(DB_FILE, timeout=10)

    df = pd.read_sql_query(
        "SELECT symbol, side, status, created_at "
        "FROM trades WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)') "
        "ORDER BY id ASC",
        conn
    )

    conn.close()

    if df.empty:
        return jsonify({
            'dates': [],
            'pnl': [],
            'win_rate': 0,
            'wins': 0,
            'losses': 0,
            'net_pnl': 0,
            'recent': []
        })

    margin = CONFIG.get('MARGIN_PER_TRADE', 2.0)
    df['pnl_value'] = df['status'].apply(
        lambda x: margin if 'TP' in x else -margin
    )
    df['cum_pnl'] = df['pnl_value'].cumsum()
    df['date_only'] = pd.to_datetime(
        df['created_at']
    ).dt.strftime('%d %b %H:%M')

    wins = len(df[df['status'] == 'CLOSED (TP)'])
    losses = len(df[df['status'] == 'CLOSED (SL)'])
    total = wins + losses

    win_rate = round((wins / total) * 100, 1) if total > 0 else 0
    net_pnl = round(df['cum_pnl'].iloc[-1], 2) if not df.empty else 0
    recent = df.tail(10)[::-1].to_dict('records')

    return jsonify({
        'dates': df['date_only'].tolist(),
        'pnl': df['cum_pnl'].tolist(),
        'win_rate': win_rate,
        'wins': wins,
        'losses': losses,
        'net_pnl': net_pnl,
        'recent': recent
    })


def notify_runtime_status(new_status, text):
    global last_runtime_status
    if last_runtime_status != new_status:
        last_runtime_status = new_status
        send_telegram(text)


def main_loop():
    global daily_sl_count, IS_BOT_PAUSED, DAILY_LOSS_ENABLED
    DAILY_LOSS_ENABLED = bool(get_db_setting('daily_loss_enabled', 1.0))

    last_hb = time.time()
    send_telegram("🚀 <b>ALCHEMIST v2 INITIALIZED</b>\n🟢 Trading engine siap. Menunggu kondisi scan.")

    while True:
        try:
            now = datetime.now(WIB)

            if now.hour == 7 and now.minute == 0 and now.second < 15:
                daily_sl_count = 0
                if IS_BOT_PAUSED:
                    IS_BOT_PAUSED = False
                    send_telegram("🌅 <b>NEW DAY RESET</b>\nBot otomatis kembali aktif.")

            if time.time() - last_hb >= 21600:
                send_telegram(
                    "💓 <b>SYSTEM PING</b>\n"
                    f"Bot: {'⏸ PAUSED' if IS_BOT_PAUSED else '🟢 RUNNING'}\n"
                    f"Mode: {'🧪 DRY RUN' if CONFIG['DRY_RUN'] else '⚠️ REAL MONEY'}\n"
                    f"CEX aktif: {len(exchanges)}"
                )
                last_hb = time.time()

            check_and_update_daily_sl()
            update_orders_and_positions()

            if IS_BOT_PAUSED:
                notify_runtime_status(
                    'PAUSED',
                    "⏸️ <b>TRADING PAUSED</b>\nPembukaan trade baru dihentikan. Monitoring posisi tetap berjalan."
                )
            elif is_kill_zone():
                if not exchanges:
                    notify_runtime_status(
                        'NO_CEX',
                        "⚠️ <b>SCAN TIDAK BERJALAN</b>\nTidak ada CEX yang sedang ON. Aktifkan CEX setelah API valid."
                    )
                else:
                    notify_runtime_status(
                        'SCANNING',
                        "🔎 <b>SCANNING MARKET</b>\nBot sedang mencari setup sesuai logic SMC yang ada."
                    )
                    scan_markets()
            else:
                notify_runtime_status(
                    'WAITING',
                    "⏹️ <b>STOP TRADE / WAITING</b>\nDi luar kill zone. Bot tidak membuka trade baru dan menunggu sesi berikutnya."
                )

        except Exception as e:
            send_error_alert(str(e))

        time.sleep(15)


async def app_main():
    # Initialize shared state before any worker starts.
    init_db()
    load_settings()
    reload_exchanges()

    # Trading engine runs in a worker thread.
    threading.Thread(
        target=main_loop,
        name='trading-engine',
        daemon=True
    ).start()

    # Flask runs in a worker thread. use_reloader=False prevents Flask
    # from spawning a second process/thread and interfering with the bot.
    threading.Thread(
        target=lambda: app.run(
            host='0.0.0.0',
            port=5000,
            use_reloader=False
        ),
        name='flask-dashboard',
        daemon=True
    ).start()

    # Aiogram MUST stay in the main thread.
    try:
        await aiogram_polling()
    finally:
        await bot.session.close()


if __name__ == '__main__':
    try:
        asyncio.run(app_main())
    except (KeyboardInterrupt, SystemExit):
        print('ALCHEMIST stopped.', flush=True)
    except Exception as e:
        print(f'[FATAL] {e}', flush=True)
        try:
            send_error_alert(f'FATAL: {e}')
        except Exception:
            pass
