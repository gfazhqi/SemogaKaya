import os
import sys
import time
import sqlite3
import threading
import pandas as pd
import requests
import ccxt
import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
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
last_error_time = 0
ERROR_COOLDOWN = 300

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
        enabled INTEGER DEFAULT 0
    )''')
    try:
        c.execute("ALTER TABLE cex_settings ADD COLUMN enabled INTEGER DEFAULT 0")
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
            "INSERT OR IGNORE INTO cex_settings (ex_id, api_key, secret, enabled) VALUES (?, '', '', 0)",
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
        "SELECT ex_id, api_key, secret, enabled FROM cex_settings"
    ).fetchall()
    conn.close()

    exchanges.clear()

    for ex_id, api, sec, en in rows:
        if en == 1:
            try:
                exchanges[ex_id] = getattr(ccxt, ex_id)({
                    'apiKey': api if api else '',
                    'secret': sec if sec else '',
                    'options': {'defaultType': DEFAULT_CEX[ex_id]['type']},
                    'enableRateLimit': True,
                    'timeout': 5000
                })
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
        send_telegram(f"⚠️ <b>ERROR ALERT</b>\n\n<code>{str(err_msg)[:3500]}</code>")
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
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()


def is_allowed_chat(message_or_callback):
    chat = message_or_callback.chat if hasattr(message_or_callback, 'chat') else message_or_callback.message.chat
    return str(chat.id) == str(TELEGRAM_CHAT_ID)


def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Posisi", callback_data="positions"),
            InlineKeyboardButton(text="📈 Statistik", callback_data="stats")
        ],
        [
            InlineKeyboardButton(text="⚙️ Settings", callback_data="settings"),
            InlineKeyboardButton(text="🏆 Pair Perf", callback_data="pairperf")
        ],
        [
            InlineKeyboardButton(text="🏦 CEX", callback_data="cex_status"),
            InlineKeyboardButton(text="🔐 API", callback_data="api_status")
        ],
        [
            InlineKeyboardButton(text="⏸ Pause", callback_data="pause"),
            InlineKeyboardButton(text="▶️ Resume", callback_data="resume")
        ],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="menu")]
    ])


def db_read(query, params=()):
    conn = sqlite3.connect(DB_FILE, timeout=5)
    try:
        return conn.cursor().execute(query, params).fetchall()
    finally:
        conn.close()


def build_settings_text():
    cex_db = db_read("SELECT ex_id, api_key, enabled FROM cex_settings")
    mode_text = "🟢 DRY RUN" if CONFIG['DRY_RUN'] else "⚠️ REAL MONEY"
    lines = []
    for ex_id, api, en in cex_db:
        loaded = ex_id in exchanges
        lines.append(
            f"• {ex_id.upper()}: {'🟢 ON' if en else '🔴 OFF'} | "
            f"Lev: {CONFIG['LEVERAGE'].get(ex_id, 50)}x | "
            f"API: {'✅' if api else '❌'} | "
            f"Conn: {'🟢' if loaded else '⚪'}"
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
    rows = db_read("SELECT ex_id, api_key, enabled FROM cex_settings")
    if not rows:
        return "🏦 <b>CEX STATUS</b>\nBelum ada CEX."
    lines = []
    for ex_id, api, enabled in rows:
        if not enabled:
            state = "🔴 OFF"
        elif ex_id in exchanges:
            state = "🟢 LOADED"
        else:
            state = "🟡 CONFIGURED / NOT LOADED"
        lines.append(
            f"• <b>{ex_id.upper()}</b> | {state} | API {'✅' if api else '❌'} | "
            f"Lev {CONFIG['LEVERAGE'].get(ex_id, 50)}x"
        )
    return "🏦 <b>CEX STATUS</b>\n\n" + "\n".join(lines)


def build_api_status_text():
    rows = db_read("SELECT ex_id, api_key, secret, enabled FROM cex_settings")
    lines = []
    for ex_id, api, secret, enabled in rows:
        if not api or not secret:
            state = "❌ Belum diisi"
        elif ex_id in exchanges:
            state = "🟢 Tersimpan & loaded"
        else:
            state = "🟡 Tersimpan, CEX OFF / gagal load"
        lines.append(f"• {ex_id.upper()}: {state}")
    return "🔐 <b>API STATUS</b>\n\n" + ("\n".join(lines) or "Belum ada")


def validate_cex_api(ex_id, api, secret):
    if ex_id not in DEFAULT_CEX:
        return False, "CEX tidak dikenal."
    if not api or not secret:
        return False, "API key/secret kosong."
    try:
        ex = getattr(ccxt, ex_id)({
            'apiKey': api,
            'secret': secret,
            'options': {'defaultType': DEFAULT_CEX[ex_id]['type']},
            'enableRateLimit': True,
            'timeout': 8000
        })
        ex.fetch_balance()
        return True, "API valid dan berhasil terhubung."
    except Exception as e:
        return False, str(e)[:1200]


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
    if not is_allowed_chat(message): return
    parts = (message.text or '').split()
    if len(parts) != 4:
        await message.answer("⚠️ Format: /setapi <cex> <api_key> <secret>")
        return
    ex_id, api, secret = parts[1].lower(), parts[2], parts[3]
    status_msg = await message.answer(f"🔄 Memvalidasi API {ex_id.upper()}...")
    ok, detail = await asyncio.to_thread(validate_cex_api, ex_id, api, secret)
    if not ok:
        await status_msg.edit_text(
            f"❌ <b>API {ex_id.upper()} TIDAK VALID</b>\n\n<code>{detail}</code>"
        )
        return
    conn = sqlite3.connect(DB_FILE, timeout=5)
    conn.cursor().execute(
        "UPDATE cex_settings SET api_key=?, secret=? WHERE ex_id=?",
        (api, secret, ex_id)
    )
    conn.commit(); conn.close()
    reload_exchanges()
    await status_msg.edit_text(
        f"✅ <b>API {ex_id.upper()} VALID</b>\nAPI berhasil diverifikasi dan disimpan.",
        reply_markup=main_keyboard()
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
    await callback.message.edit_text(build_settings_text(), reply_markup=main_keyboard())


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
    await safe_answer(callback, "Bot di-pause")
    await callback.message.edit_text("⏸️ <b>Bot PAUSED</b>", reply_markup=main_keyboard())


@dp.callback_query(F.data == "resume")
async def cb_resume(callback: CallbackQuery):
    global IS_BOT_PAUSED
    if not is_allowed_chat(callback): return
    IS_BOT_PAUSED = False
    await safe_answer(callback, "Bot dilanjutkan")
    await callback.message.edit_text("▶️ <b>Bot RESUMED</b>", reply_markup=main_keyboard())


async def aiogram_polling():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


def telegram_polling():
    """Run Aiogram polling in its own thread. Trading logic remains separate."""
    try:
        asyncio.run(aiogram_polling())
    except Exception as e:
        print(f"[AIOGRAM ERROR] {e}", flush=True)
        send_error_alert(f"Aiogram polling: {e}")


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


def main_loop():
    global daily_sl_count, IS_BOT_PAUSED

    last_hb = time.time()
    send_telegram("🚀 <b>ALCHEMIST v2 INITIALIZED</b>")

    while True:
        try:
            now = datetime.now(WIB)

            if now.hour == 7 and now.minute == 0 and now.second < 15:
                daily_sl_count = 0
                if IS_BOT_PAUSED:
                    IS_BOT_PAUSED = False
                    send_telegram("🌅 <b>NEW DAY RESET</b>")

            if time.time() - last_hb >= 21600:
                send_telegram("💓 <b>PING</b> Sistem normal.")
                last_hb = time.time()

            check_and_update_daily_sl()
            update_orders_and_positions()

            if not IS_BOT_PAUSED and is_kill_zone():
                scan_markets()

        except Exception as e:
            send_error_alert(str(e))

        time.sleep(15)


if __name__ == '__main__':
    init_db()
    load_settings()
    reload_exchanges()

    threading.Thread(
        target=telegram_polling,
        daemon=True
    ).start()

    threading.Thread(
        target=lambda: app.run(
            host='0.0.0.0',
            port=5000
        ),
        daemon=True
    ).start()

    main_loop()
