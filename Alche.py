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
import zipfile
import shutil
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.exceptions import TelegramBadRequest
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify

# =================================================================
# CONFIGURATION & PARAMETERS
# =================================================================
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
PUBLIC_CHANNEL_ID = getattr(__import__('config'), 'PUBLIC_CHANNEL_ID', '')
VIP_JOIN_URL = getattr(__import__('config'), 'VIP_JOIN_URL', '')

SL_BUFFER_PERCENT = 0.0005
MAX_DAILY_SL = 2
DB_FILE = "trades.db"
WIB = timezone(timedelta(hours=7))

MANUAL_ALERT_PAIRS = []
CORE_PAIRS = ['BTC/USDT', 'ETH/USDT']

DEFAULT_CEX = {
    'bingx': {'type': 'swap'},
    'bitget': {'type': 'swap'},
    'bybit': {'type': 'linear'},
    'binance': {'type': 'future'}
}

exchanges = {}

# Serialize trade-state DB operations so Telegram cleanup cannot race with
# the background position updater and resurrect a cancelled queue item.
DB_LOCK = threading.RLock()
MAIN_ASYNC_LOOP = None
PUBLIC_LAST_DAILY_REPORT_DATE = None
PUBLIC_LAST_WEEKLY_REPORT_DATE = None

# Scanner runtime state.
# M15 strategy is evaluated on closed candles; there is no reason to hammer
# the exchange every 15 seconds with hundreds of OHLCV requests.
EXCHANGE_SCAN_NEXT = {}
EXCHANGE_MARKETS = {}
EXCHANGE_LAST_CANDLE = {}
EXCHANGE_SCAN_CURSOR = {}
EXCHANGE_REQUEST_NEXT = {}
BINGX_BACKOFF_SECONDS = 30
BINGX_REQUEST_SPACING = 0.35
SCANNER_MIN_INTERVAL = 15


CONFIG = {
    'MARGIN_PER_TRADE': 2.0,
    'MAX_CONCURRENT_POSITIONS': 2,
    'LEVERAGE': {ex: 50 for ex in DEFAULT_CEX.keys()},
    'DRY_RUN': True,
    'MIN_TRADES': 5,
    'MIN_WR': 40.0,
    'DAILY_LOSS_LIMIT': True
}

IS_BOT_PAUSED = False
daily_sl_count = 0
last_error_time = 0
ERROR_COOLDOWN = 300
last_runtime_status = None
ERROR_HISTORY = []
ERROR_HISTORY_MAX = 50
BOT_STARTED_AT = time.time()
EXCHANGE_RUNTIME = {}
LAST_EVENT_TEXT = 'Bot initialized'
LAST_EVENT_TIME = time.time()

# Track the latest Telegram main-menu message per chat so /menu can
# replace the previous menu instead of stacking messages.
LAST_MENU_MESSAGE = {}

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
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('daily_loss_limit', 1.0)")

    for ex in DEFAULT_CEX.keys():
        c.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, 50.0)",
            (f'lev_{ex}',)
        )
        c.execute(
            "INSERT OR IGNORE INTO cex_settings (ex_id, api_key, secret, enabled, account_label, account_info, password) VALUES (?, '', '', 0, '', '', '')",
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
        elif row[0] == 'daily_loss_limit':
            CONFIG['DAILY_LOSS_LIMIT'] = bool(row[1])
        elif row[0].startswith('lev_'):
            CONFIG['LEVERAGE'][row[0].split('_', 1)[1]] = int(row[1])

    conn.close()


def reload_exchanges():
    global exchanges, EXCHANGE_MARKETS, EXCHANGE_SCAN_NEXT, EXCHANGE_LAST_CANDLE, EXCHANGE_SCAN_CURSOR, EXCHANGE_REQUEST_NEXT

    rows = db_read(
        "SELECT ex_id, api_key, secret, enabled, password FROM cex_settings"
    )
    EXCHANGE_SCAN_CURSOR = {}
    EXCHANGE_REQUEST_NEXT = {}

    exchanges.clear()
    EXCHANGE_MARKETS.clear()
    EXCHANGE_SCAN_NEXT.clear()
    EXCHANGE_LAST_CANDLE.clear()

    for ex_id, api, sec, en, password in rows:
        if en != 1:
            continue
        try:
            params = {
                'apiKey': api or '',
                'secret': sec or '',
                'options': {'defaultType': DEFAULT_CEX[ex_id]['type']},
                'enableRateLimit': True,
                'timeout': 8000
            }
            if ex_id == 'bingx':
                params['rateLimit'] = 350
            if ex_id == 'bitget':
                # CCXT/Bitget uses `password` for API passphrase.
                params['password'] = password or ''

            ex = getattr(ccxt, ex_id)(params)
            # Load the market catalogue once. The scanner will use only
            # active USDT-settled perpetual markets from this catalogue.
            ex.load_markets()
            exchanges[ex_id] = ex
            EXCHANGE_MARKETS[ex_id] = ex.markets
        except Exception as e:
            send_error_alert(f"Load Ex [{ex_id}]: {e}")


def update_db_setting(key, value):
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.cursor().execute(
        "UPDATE settings SET value=? WHERE key=?",
        (value, key)
    )
    conn.commit()
    conn.close()



# =================================================================
# TEMPORARY TELEGRAM BACKUP
# Backup files are created only in the system temp directory.
# They are deleted automatically after the Telegram message is
# removed (default: 5 minutes). No permanent backup is kept on VPS.
# =================================================================
BACKUP_MESSAGE_TTL_SECONDS = 300

async def create_temp_db_backup():
    """Create a consistent SQLite backup in the OS temp directory."""
    import sqlite3
    fd, temp_path = tempfile.mkstemp(prefix="alche_backup_", suffix=".db")
    os.close(fd)
    try:
        src_conn = sqlite3.connect(DB_FILE, timeout=15)
        dst_conn = sqlite3.connect(temp_path, timeout=15)
        with dst_conn:
            src_conn.backup(dst_conn)
        dst_conn.close()
        src_conn.close()
        return temp_path
    except Exception:
        try:
            src_conn.close()
        except Exception:
            pass
        try:
            Path(temp_path).unlink(missing_ok=True)
        except Exception:
            pass
        raise

async def send_temporary_backup(chat_id):
    """Send DB backup to Telegram and delete both message/file after 5 min."""
    temp_path = await create_temp_db_backup()
    backup_message = None
    try:
        from aiogram.types import FSInputFile
        backup_message = await bot.send_document(
            chat_id=chat_id,
            document=FSInputFile(temp_path),
            caption=(
                "💾 <b>BACKUP DATABASE</b>\n\n"
                "⚠️ Backup ini hanya sementara.\n"
                "🗑️ Pesan dan file temporary akan dihapus otomatis "
                "setelah <b>5 menit</b>.\n"
                "📥 Simpan/download sekarang jika diperlukan untuk migrasi VPS."
            ),
        )
        await asyncio.sleep(BACKUP_MESSAGE_TTL_SECONDS)
        try:
            await bot.delete_message(chat_id=chat_id, message_id=backup_message.message_id)
        except Exception:
            pass
    finally:
        try:
            Path(temp_path).unlink(missing_ok=True)
        except Exception:
            pass


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
    global last_error_time, ERROR_HISTORY
    text = str(err_msg)[:3500]
    ERROR_HISTORY.append({
        'time': datetime.now(WIB).strftime('%d/%m/%Y %H:%M:%S'),
        'message': text
    })
    ERROR_HISTORY = ERROR_HISTORY[-ERROR_HISTORY_MAX:]
    if time.time() - last_error_time > ERROR_COOLDOWN:
        safe_err = html.escape(text)
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


def get_open_position_contracts(ex, symbol):
    """Return the currently open contract amount for one symbol.

    Uses CCXT unified positions first. This is only used for a manual emergency
    close, so we prefer an exact live position size over the locally calculated
    entry quantity.
    """
    positions = ex.fetch_positions([symbol])
    if not positions:
        return 0.0, None

    best = None
    for pos in positions:
        if pos.get('symbol') != symbol:
            continue
        contracts = pos.get('contracts')
        if contracts is None:
            contracts = pos.get('amount')
        try:
            contracts = abs(float(contracts or 0.0))
        except (TypeError, ValueError):
            contracts = 0.0
        if contracts <= 0:
            continue
        best = (contracts, pos.get('side'))
        break

    return best if best else (0.0, None)


def close_open_trade(trade_id):
    """Manually close one OPEN trade, intended for emergency/news exits.

    DRY RUN only changes the local simulation state. REAL mode sends a market
    order in the opposite direction with reduceOnly=True, using the live
    position size returned by the exchange.
    """
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
            row = conn.execute(
                "SELECT exchange_id, symbol, side, order_id, status "
                "FROM trades WHERE id=?",
                (trade_id,)
            ).fetchone()
            if not row:
                return False, "Trade tidak ditemukan."

            ex_id, symbol, side, order_id, status = row
            if status != 'OPEN':
                return False, f"Trade bukan OPEN (status: {status})."

            is_sim = str(order_id).startswith('sim_')
            ex = exchanges.get(ex_id)

            if CONFIG.get('DRY_RUN', True) or is_sim:
                cur = conn.execute(
                    "UPDATE trades SET status='CLOSED (MANUAL)' "
                    "WHERE id=? AND status='OPEN'",
                    (trade_id,)
                )
                conn.commit()
                if cur.rowcount:
                    return True, f"{symbol} ditutup manual (DRY RUN)."
                return False, "Trade sudah berubah status."

            if ex is None:
                return False, f"CEX {ex_id.upper()} tidak aktif."

            contracts, live_side = get_open_position_contracts(ex, symbol)
            if contracts <= 0:
                return False, (
                    f"Posisi live {symbol} tidak ditemukan di {ex_id.upper()}. "
                    "Trade lokal tidak diubah."
                )

            close_side = 'sell' if side == 'BUY' else 'buy'
            params = {'reduceOnly': True}

            # Some exchanges need positionSide in hedge mode. If CCXT exposes
            # it, pass it through; otherwise reduceOnly is sufficient for
            # one-way mode and the exchange can reject safely.
            if live_side in ('long', 'short'):
                params['positionSide'] = live_side

            result = ex.create_order(
                symbol,
                'market',
                close_side,
                contracts,
                None,
                params
            )

            cur = conn.execute(
                "UPDATE trades SET status='CLOSED (MANUAL)' "
                "WHERE id=? AND status='OPEN'",
                (trade_id,)
            )
            conn.commit()
            if not cur.rowcount:
                return False, "Order close terkirim, tetapi status lokal sudah berubah."

            return True, (
                f"{symbol} ditutup manual di {ex_id.upper()} "
                f"({contracts:g} kontrak)."
            )
        except Exception as e:
            conn.rollback()
            return False, str(e)[:900]
        finally:
            conn.close()


def close_all_open_trades():
    """Emergency-close every currently OPEN trade, one by one."""
    rows = db_read(
        "SELECT id FROM trades WHERE status='OPEN' ORDER BY id ASC"
    )
    results = []
    for (trade_id,) in rows:
        ok, detail = close_open_trade(trade_id)
        results.append((ok, detail))
    return results


def cancel_all_pending_orders_on_pause():
    """Cancel only active PENDING orders and make the DB state authoritative.

    The previous version could race with update_orders_and_positions(): the
    updater could read a PENDING row, cleanup could mark it cancelled, and the
    updater could then write the old state back. Conditional UPDATEs plus a
    process-local DB lock prevent that resurrection.
    """
    changed = 0
    try:
        with DB_LOCK:
            conn = sqlite3.connect(DB_FILE, timeout=10)
            try:
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
                            # The exchange order may already be gone; the local
                            # queue must still stop treating it as PENDING.
                            pass

                    cur = c.execute(
                        "UPDATE trades SET status='CANCELLED (Auto/Manual)' "
                        "WHERE id=? AND status='PENDING'",
                        (t_id,)
                    )
                    changed += cur.rowcount

                conn.commit()
            finally:
                conn.close()

        send_telegram(
            f"🧹 <b>CLEANUP</b> {changed} pending order dibatalkan!"
            if changed else
            "🧹 <b>CLEANUP</b> Tidak ada pending order aktif."
        )
    except Exception as e:
        send_error_alert(f"Cleanup pending orders: {e}")


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

        if CONFIG.get('DAILY_LOSS_LIMIT', True) and daily_sl_count >= MAX_DAILY_SL and not IS_BOT_PAUSED:
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


def _fetch_last_closed_m15(ex_instance, symbol):
    """Return the latest CLOSED M15 candle."""
    try:
        bars = ex_instance.fetch_ohlcv(symbol, '15m', limit=3)
        if not bars or len(bars) < 2:
            return None
        # Last candle may still be forming; use the previous one.
        return bars[-2]
    except Exception:
        return None


def _pending_price_state(ex_instance, symbol, side, entry, sl, tp):
    """
    Validate a PENDING setup using M15 OHLC, not a low-TF ticker.
    This keeps the strategy timeframe at M15 and avoids 1m noise.

    Important:
    - If Entry is touched by the M15 candle, it becomes FILLED.
    - If Entry is NOT touched but TP is already beyond the candle range,
      it remains PENDING; we only call TP-first when the candle proves that
      TP was crossed without reaching Entry.
    - If the same candle contains both Entry and TP/SL, Entry wins because
      the pending order must be considered filled before position management.
    """
    try:
        bars = ex_instance.fetch_ohlcv(symbol, '15m', limit=3)
        if not bars or len(bars) < 2:
            return 'PENDING'

        candle = bars[-2]  # latest closed M15
        high = float(candle[2])
        low = float(candle[3])
        entry = float(entry)
        sl = float(sl)
        tp = float(tp)

        if side == 'BUY':
            entry_hit = low <= entry
            tp_crossed = high >= tp
            sl_crossed = low <= sl

            # Entry has priority on a candle that contains entry.
            if entry_hit:
                return 'OPEN'

            # No entry touch: only mark TP/SL first when the closed candle
            # proves price moved beyond that level.
            if tp_crossed:
                return 'MISSED (TP First)'
            if sl_crossed:
                return 'CANCELLED (SL First)'

        else:  # SELL
            entry_hit = high >= entry
            tp_crossed = low <= tp
            sl_crossed = high >= sl

            if entry_hit:
                return 'OPEN'
            if tp_crossed:
                return 'MISSED (TP First)'
            if sl_crossed:
                return 'CANCELLED (SL First)'

        return 'PENDING'
    except Exception:
        return 'PENDING'


def update_orders_and_positions():
    """Advance active trades without allowing stale rows to resurrect.

    Every state transition is conditional on the row still having the state
    that was read. This is important when /cancelall or the daily-loss cleanup
    runs at the same time as the background monitor.
    """
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        try:
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
                    ex = exchanges[ex_id]
                    n_bars = bars + 1
                    is_sim = str(ord_id).startswith('sim_')
                    tag = " (DRY RUN)" if is_sim else ""

                    if status == 'PENDING':
                        n_stat = _pending_price_state(ex, sym, side, en, sl, tp)

                        if n_stat == 'OPEN':
                            send_telegram(
                                f"🔔 <b>ORDER FILLED{tag}</b> {sym} di {ex_id.upper()}!"
                            )
                        elif n_stat != 'PENDING':
                            if ('CANCEL' in n_stat or 'MISS' in n_stat or 'INV' in n_stat) and not is_sim:
                                try:
                                    ex.cancel_order(ord_id, sym)
                                except Exception:
                                    pass

                            send_telegram(
                                f"⚪ <b>AUTO-CANCEL{tag}</b> {sym}\n"
                                f"Alasan: {n_stat}"
                            )

                        if n_stat != 'PENDING':
                            # Do not overwrite a concurrent CANCEL ALL.
                            c.execute(
                                "UPDATE trades SET status=?, bars_count=? "
                                "WHERE id=? AND status='PENDING'",
                                (n_stat, n_bars, t_id)
                            )
                        else:
                            c.execute(
                                "UPDATE trades SET bars_count=? "
                                "WHERE id=? AND status='PENDING'",
                                (n_bars, t_id)
                            )

                    elif status == 'OPEN':
                        curr_price = ex.fetch_ticker(sym)['last']

                        if (side == 'BUY' and curr_price >= tp) or (
                            side == 'SELL' and curr_price <= tp
                        ):
                            c.execute(
                                "UPDATE trades SET status='CLOSED (TP)' "
                                "WHERE id=? AND status='OPEN'",
                                (t_id,)
                            )
                            if c.rowcount:
                                send_telegram(
                                    f"🎉 <b>TAKE PROFIT{tag}</b> {sym} | +1R 🚀"
                                )

                        elif (side == 'BUY' and curr_price <= sl) or (
                            side == 'SELL' and curr_price >= sl
                        ):
                            c.execute(
                                "UPDATE trades SET status='CLOSED (SL)' "
                                "WHERE id=? AND status='OPEN'",
                                (t_id,)
                            )
                            if c.rowcount:
                                send_telegram(
                                    f"❌ <b>STOP LOSS{tag}</b> {sym} | -1R 🔻"
                                )

                except Exception as e:
                    # Keep one bad symbol from aborting the whole monitor pass.
                    send_error_alert(f"Position monitor [{ex_id}:{sym}]: {e}")

            conn.commit()
        finally:
            conn.close()


# =================================================================
# RUNTIME MONITORING
# =================================================================
def set_runtime_event(text):
    global LAST_EVENT_TEXT, LAST_EVENT_TIME
    LAST_EVENT_TEXT = str(text)[:500]
    LAST_EVENT_TIME = time.time()


def _fmt_uptime(seconds):
    seconds = max(0, int(seconds))
    d, seconds = divmod(seconds, 86400)
    h, seconds = divmod(seconds, 3600)
    m, _ = divmod(seconds, 60)
    if d:
        return f"{d}d {h}h {m}m"
    return f"{h:02d}h {m:02d}m"


def build_runtime_status_text():
    mode = '🧪 DRY RUN' if CONFIG.get('DRY_RUN', True) else '⚠️ REAL'
    engine = '⏸ PAUSED' if IS_BOT_PAUSED else '🟢 RUNNING'
    kill = '🟢 ACTIVE' if is_kill_zone() else '🔴 INACTIVE'
    pending = db_read("SELECT COUNT(*) FROM trades WHERE status='PENDING'")[0][0]
    opened = db_read("SELECT COUNT(*) FROM trades WHERE status='OPEN'")[0][0]
    total_today = db_read(
        "SELECT COUNT(*) FROM trades WHERE created_at >= date('now','start of day')"
    )[0][0]
    lines = [
        "🤖 <b>ALCHEMIST — RUNTIME STATUS</b>",
        "",
        f"{engine} | {mode}",
        f"⏱ Uptime: {_fmt_uptime(time.time() - BOT_STARTED_AT)}",
        f"🧠 Strategy: SMC M15 + H1 EMA50 + Daily Open",
        f"🕐 Kill Zone: {kill}",
        "",
        "🔎 <b>SCANNER PER CEX</b>",
    ]
    if EXCHANGE_RUNTIME:
        for ex_id in sorted(EXCHANGE_RUNTIME):
            st = EXCHANGE_RUNTIME[ex_id]
            state = st.get('state', 'IDLE')
            icon = '🟢' if state == 'SCANNING' else ('⚠️' if state == 'ERROR' else '⏸')
            last = st.get('last_scan', '-')
            reason = st.get('last_reason', '')
            line = f"• {ex_id.upper()}: {icon} {state} | Last: {last}"
            if reason:
                line += f"\n  ↳ {html.escape(reason[:140])}"
            lines.append(line)
    else:
        lines.append('• Belum ada CEX aktif.')

    lines += [
        "",
        "📊 <b>TRADE</b>",
        f"• OPEN: {opened} | PENDING: {pending}",
        f"• Today created: {total_today}",
        f"• Daily SL: {daily_sl_count}/{MAX_DAILY_SL} "
        f"({'🟢 ON' if CONFIG.get('DAILY_LOSS_LIMIT', True) else '🔴 OFF'})",
        "",
        f"⚠️ Last event: {html.escape(LAST_EVENT_TEXT[:220])}",
        f"🕒 {datetime.fromtimestamp(LAST_EVENT_TIME, WIB).strftime('%d/%m/%Y %H:%M:%S')}",
    ]
    return "\n".join(lines)


def runtime_status_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 REFRESH", callback_data="runtime_status")],
        [InlineKeyboardButton(text="📊 POSISI", callback_data="positions")],
        [InlineKeyboardButton(text="🎛️ BOT CONTROL", callback_data="bot_control")],
        [InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="menu")]
    ])


# =================================================================
# PUBLIC CHANNEL — CRYPTO SCALP SIGNALS
# =================================================================
PUBLIC_DAILY_MIN = 3
PUBLIC_DAILY_MAX = 6
PUBLIC_STATE = {
    'date': None,
    'target': None,
    'published': 0,
    'signals': []
}

def _public_target_path():
    return Path("public_signal_target.json")

def _public_get_target(today):
    path = _public_target_path()
    try:
        obj = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
        if obj.get('date') == today:
            target = int(obj.get('target', 0))
            if PUBLIC_DAILY_MIN <= target <= PUBLIC_DAILY_MAX:
                return target
    except Exception:
        pass
    import random
    target = random.randint(PUBLIC_DAILY_MIN, PUBLIC_DAILY_MAX)
    try:
        path.write_text(json.dumps({'date': today, 'target': target}), encoding='utf-8')
    except Exception:
        pass
    return target

def _public_reset_if_new_day():
    today = datetime.now(WIB).date().isoformat()
    if PUBLIC_STATE['date'] != today:
        PUBLIC_STATE['date'] = today
        PUBLIC_STATE['target'] = _public_get_target(today)
        PUBLIC_STATE['published'] = 0
        PUBLIC_STATE['signals'] = []
        hist = _public_load_history()
        today_items = [x for x in hist if x.get('date') == today]
        if today_items:
            PUBLIC_STATE['signals'] = today_items
            PUBLIC_STATE['published'] = len(today_items)
    return PUBLIC_STATE

def _public_save_history():
    path = Path("public_signal_history.json")
    try:
        path.write_text(json.dumps(PUBLIC_STATE['signals'], ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        pass

def _public_load_history():
    path = Path("public_signal_history.json")
    try:
        data = json.loads(path.read_text(encoding='utf-8')) if path.exists() else []
        return data if isinstance(data, list) else []
    except Exception:
        return []

def _public_record_outcome(signal, status, r):
    signal['status'] = status
    signal['r'] = float(r)
    _public_save_history()

def _public_crypto_symbol(sym):
    s = str(sym).upper()
    if '/USDT' not in s:
        return False
    blocked = ('XAU', 'XAG', 'WTI', 'USOIL', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'NZD')
    if any(x in s for x in blocked):
        return False
    # Reject leveraged-token styles such as BTCUP, BTCDOWN, 2L/2S/3L/3S.
    base = s.split('/')[0]
    if any(x in base for x in ('UP', 'DOWN', 'BULL', 'BEAR')) or re.search(r'\d+[LS]$', base):
        return False
    return True

def _public_keyboard():
    rows = []
    if VIP_JOIN_URL:
        rows.append([InlineKeyboardButton(text="👑 JOIN VIP — AUTO TRADE", url=VIP_JOIN_URL)])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

def _public_signal_text(n, total, sym, side, ex_id, entry, sl):
    risk = abs(float(entry) - float(sl))
    tp1 = float(entry) + risk if side == 'BUY' else float(entry) - risk
    tp2 = float(entry) + (2 * risk) if side == 'BUY' else float(entry) - (2 * risk)
    now = datetime.now(WIB)
    return (
        f"📣 <b>SCALP TRADE #{n}/{total}</b>\n\n"
        f"📅 {now.strftime('%d %B %Y')}\n"
        f"🕐 {now.strftime('%H:%M:%S')} WIB\n"
        f"🪙 <b>{html.escape(str(sym))}</b>\n"
        f"🏦 {html.escape(str(ex_id).upper())}\n"
        f"📈 <b>{side}</b>\n\n"
        f"🎯 Entry: <code>{float(entry):.8g}</code>\n"
        f"🛑 SL: <code>{float(sl):.8g}</code>\n"
        f"💰 TP1: <code>{tp1:.8g}</code> (1R)\n"
        f"🚀 TP2: <code>{tp2:.8g}</code> (2R)\n\n"
        f"⚡ <b>SCALP TRADE</b>\n"
        f"📊 Timeframe: M15"
    ), tp1, tp2

async def publish_public_signal(sym, side, ex_id, entry, sl):
    if not PUBLIC_CHANNEL_ID or not _public_crypto_symbol(sym):
        return None
    state = _public_reset_if_new_day()
    if state['published'] >= state['target']:
        return None
    if not side or float(entry) <= 0 or float(sl) <= 0:
        return None

    n = state['published'] + 1
    text_msg, tp1, tp2 = _public_signal_text(
        n, state['target'], sym, side, ex_id, entry, sl
    )
    kwargs = {'chat_id': PUBLIC_CHANNEL_ID, 'text': text_msg}
    kb = _public_keyboard()
    if kb:
        kwargs['reply_markup'] = kb

    try:
        sent = await bot.send_message(**kwargs)
    except Exception:
        return None

    state['published'] = n
    state['signals'].append({
        'n': n, 'symbol': sym, 'side': side, 'exchange': ex_id,
        'entry': float(entry), 'sl': float(sl),
        'tp1': tp1, 'tp2': tp2,
        'status': 'UNRESOLVED', 'r': 0.0,
        'message_id': getattr(sent, 'message_id', None),
        'date': state['date']
    })
    _public_save_history()
    return sent

def _public_daily_report_text():
    state = _public_reset_if_new_day()
    items = state['signals']
    wins = sum(1 for x in items if x['status'] == 'WIN')
    losses = sum(1 for x in items if x['status'] == 'LOSS')
    unresolved = sum(1 for x in items if x['status'] == 'UNRESOLVED')
    decided = wins + losses
    wr = wins / decided * 100 if decided else 0.0
    net_r = sum(float(x.get('r', 0.0)) for x in items)
    return (
        f"🏁 <b>DAILY SCALP REPORT</b>\n\n"
        f"📅 {datetime.now(WIB).strftime('%d %B %Y')}\n"
        f"🪙 Crypto Perpetual\n"
        f"📊 Timeframe: M15\n\n"
        f"📢 Public Signal: {len(items)}/{state['target']}\n"
        f"✅ WIN: {wins}\n"
        f"❌ LOSS: {losses}\n"
        f"⏳ Unresolved: {unresolved}\n\n"
        f"🏆 Win Rate: <b>{wr:.2f}%</b>\n"
        f"📈 Net Result: <b>{net_r:+.2f}R</b>\n\n"
        f"🎯 TP1 (1R): {sum(1 for x in items if x['status']=='WIN' and x.get('r') == 1)}\n"
        f"🚀 TP2 (2R): {sum(1 for x in items if x['status']=='WIN' and x.get('r') == 2)}\n"
        f"🛑 SL: {losses}\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📌 <b>STATUS: TRADE SELESAI</b>\n\n"
        f"Semua public signal hari ini telah selesai.\n"
        f"Public signal akan kembali tersedia besok."
    )

async def update_public_signal_outcomes():
    """Evaluate unresolved public signals: 1R/2R = WIN, SL = LOSS."""
    state = _public_reset_if_new_day()
    for sig in state['signals']:
        if sig.get('status') != 'UNRESOLVED':
            continue
        ex = exchanges.get(sig.get('exchange'))
        sym = sig.get('symbol')
        if ex is None or not sym:
            continue
        try:
            px = float(ex.fetch_ticker(sym)['last'])
            side = sig.get('side')
            if side == 'BUY':
                if px <= float(sig['sl']):
                    _public_record_outcome(sig, 'LOSS', -1)
                elif px >= float(sig['tp2']):
                    _public_record_outcome(sig, 'WIN', 2)
                elif px >= float(sig['tp1']):
                    _public_record_outcome(sig, 'WIN', 1)
            elif side == 'SELL':
                if px >= float(sig['sl']):
                    _public_record_outcome(sig, 'LOSS', -1)
                elif px <= float(sig['tp2']):
                    _public_record_outcome(sig, 'WIN', 2)
                elif px <= float(sig['tp1']):
                    _public_record_outcome(sig, 'WIN', 1)
        except Exception:
            continue

async def send_public_daily_report(force=False):
    global PUBLIC_LAST_DAILY_REPORT_DATE
    if not PUBLIC_CHANNEL_ID:
        return
    state = _public_reset_if_new_day()
    if not state['signals']:
        return
    if not force:
        if state['published'] < state['target']:
            return
        if any(x.get('status') == 'UNRESOLVED' for x in state['signals']):
            return
    today = state['date']
    if PUBLIC_LAST_DAILY_REPORT_DATE == today and not force:
        return
    PUBLIC_LAST_DAILY_REPORT_DATE = today
    kwargs = {'chat_id': PUBLIC_CHANNEL_ID, 'text': _public_daily_report_text()}
    kb = _public_keyboard()
    if kb:
        kwargs['reply_markup'] = kb
    try:
        await bot.send_message(**kwargs)
    except Exception:
        PUBLIC_LAST_DAILY_REPORT_DATE = None

async def _public_weekly_report():
    global PUBLIC_LAST_WEEKLY_REPORT_DATE
    """Weekly report is sent Sunday morning from the persisted public history."""
    # Uses only signals recorded by this public-channel layer.
    if not PUBLIC_CHANNEL_ID:
        return
    now = datetime.now(WIB)
    if now.weekday() != 6:
        return
    sunday_key = now.date().isoformat()
    if PUBLIC_LAST_WEEKLY_REPORT_DATE == sunday_key:
        return
    # Weekly aggregation can be populated by the daily history hook below.
    wins = losses = unresolved = 0
    net_r = 0.0
    # Current-day signals are included; prior days require persistence.
    # We keep a small JSON file, not trade DB, so public stats don't alter trading.
    path = Path("public_signal_history.json")
    try:
        history = json.loads(path.read_text(encoding='utf-8')) if path.exists() else []
    except Exception:
        history = []
    monday = (now - timedelta(days=now.weekday())).date()
    for x in history:
        try:
            d = datetime.fromisoformat(x['date']).date()
        except Exception:
            continue
        if monday <= d <= now.date():
            status = x.get('status')
            if status == 'WIN':
                wins += 1
            elif status == 'LOSS':
                losses += 1
            else:
                unresolved += 1
            net_r += float(x.get('r', 0.0))
    total = wins + losses + unresolved
    decided = wins + losses
    wr = wins / decided * 100 if decided else 0.0
    kb = _public_keyboard()
    msg = (
        f"🏆 <b>WEEKLY SCALP REPORT</b>\n\n"
        f"📅 Periode: {monday.strftime('%d %B %Y')} – {now.strftime('%d %B %Y')}\n"
        f"🪙 Crypto Perpetual\n"
        f"📊 Timeframe: M15\n\n"
        f"📢 Total Public Signal: {total}\n"
        f"✅ WIN: {wins}\n"
        f"❌ LOSS: {losses}\n"
        f"⏳ Unresolved: {unresolved}\n\n"
        f"🏆 Win Rate: <b>{wr:.2f}%</b>\n"
        f"📈 Net Result: <b>{net_r:+.2f}R</b>"
    )
    try:
        kwargs = {'chat_id': PUBLIC_CHANNEL_ID, 'text': msg}
        if kb:
            kwargs['reply_markup'] = kb
        await bot.send_message(**kwargs)
        PUBLIC_LAST_WEEKLY_REPORT_DATE = sunday_key
    except Exception:
        pass


def _submit_async(coro):
    """Submit a coroutine from the synchronous trading worker to aiogram's loop."""
    loop = MAIN_ASYNC_LOOP
    if loop is None or not loop.is_running():
        try:
            coro.close()
        except Exception:
            pass
        return None
    try:
        return asyncio.run_coroutine_threadsafe(coro, loop)
    except Exception:
        try:
            coro.close()
        except Exception:
            pass
        return None

# =================================================================
# MARKET SCANNER
# =================================================================
# LOGIC TRADE TIDAK DIUBAH
def _is_retryable_market_error(ex_id, exc):
    text = str(exc).lower()
    return (
        ex_id == 'bingx' and (
            '100410' in text or
            '请稍后重试' in text or
            'try again later' in text or
            'too many requests' in text or
            'rate limit' in text
        )
    )


def _valid_usdt_swap_symbols(ex_id, ex):
    """Return active USDT-settled perpetual symbols only.

    This intentionally avoids random/expired/non-contract instruments that
    can appear in fetch_tickers(). The strategy itself remains M15/H1/D1.
    """
    markets = EXCHANGE_MARKETS.get(ex_id) or getattr(ex, 'markets', {}) or {}
    symbols = []

    for sym, m in markets.items():
        if not isinstance(m, dict):
            continue
        if not m.get('active', True):
            continue
        if m.get('contract') is not True:
            continue
        if m.get('swap') is not True:
            continue
        if str(m.get('quote', '')).upper() != 'USDT':
            continue
        if str(m.get('settle', '')).upper() != 'USDT':
            continue
        # Skip obvious test/expired/legacy instruments.
        if any(x in sym.upper() for x in ('TEST', '1000', 'BULL', 'BEAR')):
            continue
        symbols.append(sym)

    # Core pairs first, then deterministic order. We do not cap by the first
    # 60 dictionary entries because that made some CEXs effectively invisible.
    core = [s for s in CORE_PAIRS if s in symbols]
    rest = sorted(s for s in symbols if s not in core)
    return core + rest


def _bingx_ohlcv(ex, symbol, timeframe, limit):
    """Rate-limit BingX public OHLCV calls without blocking other CEXs."""
    if getattr(ex, 'id', '') != 'bingx':
        return ex.fetch_ohlcv(symbol, timeframe, limit=limit)

    now = time.time()
    wait = EXCHANGE_REQUEST_NEXT.get('bingx', 0) - now
    if wait > 0:
        time.sleep(wait)

    try:
        data = ex.fetch_ohlcv(symbol, timeframe, limit=limit)
        EXCHANGE_REQUEST_NEXT['bingx'] = time.time() + BINGX_REQUEST_SPACING
        return data
    except Exception:
        EXCHANGE_REQUEST_NEXT['bingx'] = time.time() + BINGX_REQUEST_SPACING
        raise


def _scan_one_exchange(ex_id, ex, pair_perf):
    now = time.time()
    runtime = EXCHANGE_RUNTIME.setdefault(
        ex_id,
        {'state': 'IDLE', 'last_scan': '-', 'last_reason': '',
         'signals': 0, 'created': 0}
    )
    runtime['state'] = 'SCANNING'

    if now < EXCHANGE_SCAN_NEXT.get(ex_id, 0):
        return 0

    try:
        symbols = _valid_usdt_swap_symbols(ex_id, ex)
        runtime['last_scan'] = datetime.now(WIB).strftime('%H:%M:%S')
        runtime['last_reason'] = f'{len(symbols)} valid USDT perpetual markets'

        if not symbols:
            EXCHANGE_SCAN_NEXT[ex_id] = now + SCANNER_MIN_INTERVAL
            return 0

        # IMPORTANT:
        # Do not use one "probe" symbol to gate the entire exchange.
        # BingX can return 100410 on one request while other symbols are fine.
        # Scan a small rotating batch instead of firing hundreds of OHLCV
        # requests at once. This keeps M15 logic unchanged while avoiding
        # BingX's global/IP+UID rate limiter.
        if ex_id == 'bingx':
            batch_size = 4
            cursor = EXCHANGE_SCAN_CURSOR.get(ex_id, 0) % len(symbols)
            scan_symbols = [
                symbols[(cursor + i) % len(symbols)]
                for i in range(min(batch_size, len(symbols)))
            ]
            EXCHANGE_SCAN_CURSOR[ex_id] = (cursor + len(scan_symbols)) % len(symbols)
        else:
            # Other exchanges retain the existing broad scan behavior.
            scan_symbols = symbols

        active_count = db_read(
            "SELECT COUNT(*) FROM trades "
            "WHERE exchange_id=? AND status IN ('PENDING','OPEN')",
            (ex_id,)
        )[0][0]
        slots = max(0, CONFIG['MAX_CONCURRENT_POSITIONS'] - active_count)

        if slots <= 0:
            runtime['last_reason'] = 'Max posisi CEX tercapai'
            EXCHANGE_SCAN_NEXT[ex_id] = now + SCANNER_MIN_INTERVAL
            return 0

        created = 0
        rate_limited = 0

        for sym in scan_symbols:
            if created >= slots:
                break

            if is_manual_alert_symbol(sym):
                try:
                    m15 = _bingx_ohlcv(ex, sym, '15m', 50) if ex_id == 'bingx' else ex.fetch_ohlcv(sym, '15m', limit=50)
                    h1 = _bingx_ohlcv(ex, sym, '1h', 60) if ex_id == 'bingx' else ex.fetch_ohlcv(sym, '1h', limit=60)
                    d1 = _bingx_ohlcv(ex, sym, '1d', 2) if ex_id == 'bingx' else ex.fetch_ohlcv(sym, '1d', limit=2)
                    if m15 and h1 and d1:
                        sig, _, _, _ = analyze_smc_retrace(
                            pd.DataFrame(m15, columns=['time','open','high','low','close','vol']),
                            pd.DataFrame(h1, columns=['time','open','high','low','close','vol']),
                            d1[-1][1]
                        )
                        if sig:
                            send_telegram(f"📢 <b>FX ALERT</b> {sym} | {sig}")
                except Exception as e:
                    if _is_retryable_market_error(ex_id, e):
                        rate_limited += 1
                    continue
                continue

            if sym in pair_perf and sym not in CORE_PAIRS:
                tot_trades = pair_perf[sym]['w'] + pair_perf[sym]['l']
                if tot_trades >= CONFIG['MIN_TRADES']:
                    wr = (pair_perf[sym]['w'] / tot_trades) * 100
                    if wr < CONFIG['MIN_WR']:
                        continue

            try:
                if ex_id == 'bingx':
                    m15 = _bingx_ohlcv(ex, sym, '15m', 50)
                    h1 = _bingx_ohlcv(ex, sym, '1h', 60)
                    d1 = _bingx_ohlcv(ex, sym, '1d', 2)
                else:
                    m15 = ex.fetch_ohlcv(sym, '15m', limit=50)
                    h1 = ex.fetch_ohlcv(sym, '1h', limit=60)
                    d1 = ex.fetch_ohlcv(sym, '1d', limit=2)
            except Exception as e:
                # A BingX 100410 belongs to this symbol/request only.
                # Never abort the whole BingX scan and never put the entire
                # CEX into a 90-second dead period.
                if _is_retryable_market_error(ex_id, e):
                    rate_limited += 1
                    runtime['last_reason'] = f'{sym}: market cooldown, skip symbol'
                    continue
                continue

            if not m15 or not h1 or not d1:
                continue

            sig, en, sl, tp = analyze_smc_retrace(
                pd.DataFrame(m15, columns=['time','open','high','low','close','vol']),
                pd.DataFrame(h1, columns=['time','open','high','low','close','vol']),
                d1[-1][1]
            )

            if not sig or en <= 0:
                runtime['last_reason'] = f'{sym}: no setup / filter'
                continue

            exists = db_read(
                "SELECT 1 FROM trades "
                "WHERE exchange_id=? AND symbol=? AND side=? "
                "AND status IN ('PENDING','OPEN') LIMIT 1",
                (ex_id, sym, sig)
            )
            if exists:
                runtime['last_reason'] = f'{sym}: duplicate setup'
                continue

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
                    send_error_alert(f"Order Fails {ex_id}/{sym}: {e}")
                    continue

            conn = sqlite3.connect(DB_FILE, timeout=10)
            try:
                conn.execute(
                    "INSERT INTO trades "
                    "(exchange_id, symbol, order_type, side, entry_price, "
                    "sl_price, tp_price, order_id, status) "
                    "VALUES (?, ?, 'LIMIT', ?, ?, ?, ?, ?, 'PENDING')",
                    (ex_id, sym, sig, en, sl, tp, ord_id)
                )
                conn.commit()
            finally:
                conn.close()

            send_telegram(
                f"🎯 <b>{mode}</b>\n"
                f"CEX: {ex_id.upper()} | {sym} | {sig}\n"
                f"Entry: {en:.5f}"
            )
            try:
                # Public channel is broadcast-only; this never opens or changes a trade.
                _submit_async(publish_public_signal(sym, sig, ex_id, en, sl))
            except Exception:
                pass

            created += 1
            runtime['signals'] = runtime.get('signals', 0) + 1
            runtime['created'] = runtime.get('created', 0) + 1
            runtime['last_reason'] = f'{sym}: {sig} setup created'
            set_runtime_event(f'{ex_id.upper()} {sym} {sig} setup created')

        if rate_limited:
            runtime['last_reason'] = (
                f'{rate_limited} symbol request kena 100410, '
                f'skip lokal; scanner tetap jalan'
            )

        runtime['state'] = 'SCANNING'
        EXCHANGE_SCAN_NEXT[ex_id] = time.time() + SCANNER_MIN_INTERVAL
        return created

    except Exception as e:
        runtime['state'] = 'ERROR'
        runtime['last_reason'] = str(e)[:160]
        set_runtime_event(f'{ex_id.upper()} scanner error')
        # Only a genuine exchange-level failure gets a short retry delay.
        EXCHANGE_SCAN_NEXT[ex_id] = time.time() + SCANNER_MIN_INTERVAL
        if _is_retryable_market_error(ex_id, e):
            send_error_alert(
                f"Scan Ex [{ex_id}]: market data cooldown "
                f"(skip batch, scanner lanjut)"
            )
        else:
            send_error_alert(f"Scan Ex [{ex_id}]: {e}")
        return 0

def scan_markets():
    """Scan every enabled CEX independently without changing SMC formulas."""
    try:
        pair_perf = {}
        rows = db_read(
            "SELECT symbol, status FROM trades "
            "WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)')"
        )
        for sym, stat in rows:
            pair_perf.setdefault(sym, {'w': 0, 'l': 0})
            if 'TP' in stat:
                pair_perf[sym]['w'] += 1
            else:
                pair_perf[sym]['l'] += 1

        # Snapshot to avoid mutation while Telegram ON/OFF reloads exchanges.
        for ex_id, ex in list(exchanges.items()):
            _scan_one_exchange(ex_id, ex, pair_perf)

    except Exception as e:
        send_error_alert(f"Scanner engine: {e}")


# =================================================================
# TELEGRAM BOT - AIOGRAM + INLINE KEYBOARD + CALLBACK QUERY
# =================================================================
bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# Conversational API setup state.
# {chat_id: {stage, ex_id, api, secret, password, prompt_message_id, ack_message_id}}
# Prompt/ack message IDs are tracked so the API setup conversation can be cleaned up.
PENDING_API_SETUP = {}
PENDING_RESTORE = set()
BACKUP_DIR = Path("backups")
BACKUP_DIR.mkdir(exist_ok=True)


def is_allowed_chat(message_or_callback):
    chat = message_or_callback.chat if hasattr(message_or_callback, 'chat') else message_or_callback.message.chat
    return str(chat.id) == str(TELEGRAM_CHAT_ID)


def main_keyboard():
    # Tombol dibuat 1 baris penuh agar lebih besar dan mudah disentuh di HP.
    # Telegram tidak menyediakan API untuk mengubah warna background inline button,
    # jadi status/warna divisualisasikan dengan emoji.
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 POSISI", callback_data="positions")],
        [InlineKeyboardButton(text="📈 STATISTIK", callback_data="stats")],
        [InlineKeyboardButton(text="⚙️ SETTINGS", callback_data="settings")],
        [InlineKeyboardButton(text="🏆 PAIR PERFORMANCE", callback_data="pairperf")],
        [InlineKeyboardButton(text="🏦 CEX STATUS", callback_data="cex_status")],
        [InlineKeyboardButton(text="🛰️ RUNTIME STATUS", callback_data="runtime_status")],
        [InlineKeyboardButton(text="💾 BACKUP / RESTORE", callback_data="backup_menu")],
        [InlineKeyboardButton(text="🔐 API & AKUN", callback_data="api_menu")],
        [InlineKeyboardButton(text="🎛️ BOT CONTROL", callback_data="bot_control")],
        [InlineKeyboardButton(text="🧹 CANCEL ALL", callback_data="cancelall")],
        [InlineKeyboardButton(text="🔄 REFRESH", callback_data="menu")]
    ])


def bot_control_keyboard():
    dry = "🟢 DRY RUN: ON" if CONFIG['DRY_RUN'] else "🔴 DRY RUN: OFF / REAL"
    pause = "▶️ RESUME TRADING" if IS_BOT_PAUSED else "⏸️ PAUSE TRADING"
    loss = "🟢 DAILY LOSS: ON" if CONFIG.get('DAILY_LOSS_LIMIT', True) else "🔴 DAILY LOSS: OFF"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=dry, callback_data="dryrun_toggle")],
        [InlineKeyboardButton(text=loss, callback_data="daily_loss_toggle")],
        [InlineKeyboardButton(text=pause, callback_data="resume" if IS_BOT_PAUSED else "pause")],
        [InlineKeyboardButton(text="🧹 CANCEL ALL PENDING", callback_data="cancelall")],
        [InlineKeyboardButton(text="🚨 CLOSE ALL OPEN", callback_data="closeallopen")],
        [InlineKeyboardButton(text="🛰️ RUNTIME STATUS", callback_data="runtime_status")],
        [InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="menu")]
    ])


def settings_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 MARGIN / TRADE", callback_data="set_margin_menu")],
        [InlineKeyboardButton(text="📊 MAX POSISI", callback_data="set_maxpos_menu")],
        [InlineKeyboardButton(text="🎯 MINIMUM WIN RATE", callback_data="set_minwr_menu")],
        [InlineKeyboardButton(text="📈 MINIMUM TRADES", callback_data="set_mintrades_menu")],
        [InlineKeyboardButton(text="⚡ LEVERAGE", callback_data="set_lev_menu")],
        [InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="menu")]
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
    rows.append([InlineKeyboardButton(text="⬅️ KEMBALI", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cex_api_keyboard():
    rows = []
    for ex_id in DEFAULT_CEX:
        rows.append([InlineKeyboardButton(text=f"🔑 SET / GANTI {ex_id.upper()}", callback_data=f"api_setup:{ex_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="api_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cex_control_keyboard():
    rows = []
    db_rows = db_read("SELECT ex_id, enabled, api_key, account_label FROM cex_settings")
    for ex_id, enabled, api_key, account_label in db_rows:
        state = "🟢 ON" if enabled else "🔴 OFF"
        toggle_action = "off" if enabled else "on"
        label = account_label or "Akun belum terdeteksi"
        rows.append([InlineKeyboardButton(text=f"{ex_id.upper()} — {state} | {label}", callback_data=f"cex_toggle:{ex_id}:{toggle_action}")])
        rows.append([InlineKeyboardButton(text=f"🔑 API {ex_id.upper()}", callback_data=f"api_setup:{ex_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="api_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def api_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 SET / GANTI API", callback_data="api_setup_menu")],
        [InlineKeyboardButton(text="🏦 ON / OFF CEX", callback_data="cex_control")],
        [InlineKeyboardButton(text="📋 LIHAT API & INFO AKUN", callback_data="api_status")],
        [InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="menu")]
    ])


def leverage_keyboard():
    rows = []
    for ex_id in DEFAULT_CEX:
        rows.append([InlineKeyboardButton(
            text=f"⚡ {ex_id.upper()} — {CONFIG['LEVERAGE'].get(ex_id, 50)}x",
            callback_data=f"lev_ex:{ex_id}"
        )])
    rows.append([InlineKeyboardButton(text="⚡ SEMUA CEX", callback_data="lev_all")])
    rows.append([InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="settings")])
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
    rows.append([InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="set_lev_menu")])
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
        "SELECT id, exchange_id, symbol, side, entry_price, sl_price, tp_price, status "
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
        for trade_id, ex_id, sym, side, en, sl, tp, status in rows:
            text += (
                f"\n• <b>{ex_id.upper()} | {sym}</b> | {side}\n"
                f"  Status: {status}\n"
                f"  Entry: {en:.8f}\n"
                f"  SL: {sl:.8f}\n"
                f"  TP: {tp:.8f}\n"
            )
    return text


def positions_keyboard():
    rows = db_read(
        "SELECT id, exchange_id, symbol, status "
        "FROM trades WHERE status IN ('PENDING', 'OPEN') ORDER BY id DESC"
    )
    buttons = []
    for trade_id, ex_id, symbol, status in rows:
        if status == 'OPEN':
            buttons.append([
                InlineKeyboardButton(
                    text=f"🚨 TUTUP {ex_id.upper()} | {symbol[:24]}",
                    callback_data=f"closepos:{trade_id}"
                )
            ])
    if any(status == 'OPEN' for _, _, _, status in rows):
        buttons.append([
            InlineKeyboardButton(
                text="🚨 TUTUP SEMUA POSISI OPEN",
                callback_data="closeallopen"
            )
        ])
    buttons.extend([
        [InlineKeyboardButton(text="🧹 CANCEL ALL PENDING", callback_data="cancelall")],
        [InlineKeyboardButton(text="🔄 REFRESH", callback_data="positions")],
        [InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="menu")]
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


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
            'options': {'defaultType': DEFAULT_CEX[ex_id]['type']},
            'enableRateLimit': True,
            'timeout': 10000
        }
        if ex_id == 'bitget':
            params['password'] = password

        ex = getattr(ccxt, ex_id)(params)
        ex.load_markets()
        balance = ex.fetch_balance()
        details = extract_account_details(ex, balance)
        details = enrich_account_details(ex, details)

        # For Bitget, a successful private endpoint is not enough if the
        # key cannot access futures. Make that explicit in the validation.
        if ex_id == 'bitget':
            try:
                ex.fetch_positions()
            except Exception as e:
                detail = str(e)
                if '40014' in detail or 'Incorrect permissions' in detail:
                    return False, (
                        'Bitget API permission tidak cukup. '
                        'Aktifkan Futures/UTA position/order read & write '
                        'dan gunakan API Passphrase yang benar.'
                    ), None

        return True, "API valid dan private endpoint berhasil diakses.", details

    except Exception as e:
        detail = str(e)
        if ex_id == 'bitget' and ('40014' in detail or 'Incorrect permissions' in detail or 'future pos' in detail.lower()):
            detail += (
                " | Bitget membutuhkan izin Futures/UTA read & write "
                "untuk posisi/order dan API Passphrase yang benar."
            )
        return False, detail[:1600], None


async def safe_answer(callback: CallbackQuery, text=None, show_alert=False):
    try:
        await callback.answer(text or "OK", show_alert=show_alert)
    except Exception:
        pass


async def send_main_menu(message: Message, delete_trigger: bool = False):
    """Show one clean main menu per chat.

    When /menu is used, the command message is removed and the previous
    bot-generated main menu is removed before sending the new one.
    """
    if not is_allowed_chat(message):
        return

    chat_id = message.chat.id

    # Remove the previous bot menu, if we know its message id.
    previous_id = LAST_MENU_MESSAGE.get(chat_id)
    if previous_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=previous_id)
        except Exception:
            pass

    # Remove the /menu command itself when requested.
    if delete_trigger:
        try:
            await message.delete()
        except Exception:
            pass

    sent = await bot.send_message(
        chat_id=chat_id,
        text="🤖 <b>ALCHEMIST v2</b>\n\nPilih menu:",
        reply_markup=main_keyboard()
    )
    LAST_MENU_MESSAGE[chat_id] = sent.message_id
    return sent



async def delete_backup_message_later(chat_id: int, message_id: int, delay: int = 300):
    """Delete only the Telegram backup message after 5 minutes.
    The local backup file is intentionally kept on the VPS for migration/restore.
    """
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


def create_database_backup():
    """Create a consistent SQLite backup without copying a live WAL file blindly."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_db = BACKUP_DIR / f"trades_{ts}.db"
    zip_path = BACKUP_DIR / f"alchemist_backup_{ts}.zip"
    src = sqlite3.connect(DB_FILE, timeout=20)
    try:
        # Force WAL contents into the main DB before making the backup.
        try:
            src.execute("PRAGMA wal_checkpoint(FULL)")
        except Exception:
            pass
        dst = sqlite3.connect(str(raw_db), timeout=20)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    readme = (
        "ALCHEMIST BACKUP\n"
        "================\n"
        f"Created: {datetime.now().isoformat()}\n"
        "Contains: trades.db database (settings, CEX configuration, trade history, pending/open state).\n\n"
        "Migration:\n"
        "1. Stop the old bot before final cutover.\n"
        "2. Copy this backup to the new VPS.\n"
        "3. Restore it as trades.db (or use /restore in the bot).\n"
        "4. Copy the latest source code/config separately from your Git repo/VPS.\n"
        "5. Start only ONE bot instance.\n"
    )
    readme_path = BACKUP_DIR / f"MIGRATION_{ts}.txt"
    readme_path.write_text(readme, encoding="utf-8")
    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(raw_db, arcname="trades.db")
            z.write(readme_path, arcname="MIGRATION.txt")
    finally:
        for f in (raw_db, readme_path):
            try: f.unlink()
            except Exception: pass
    return zip_path


def validate_restore_db(path):
    """Validate that an uploaded file is an Alchemist SQLite DB before replacing it."""
    conn = sqlite3.connect(str(path), timeout=10)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        required = {"trades", "settings", "cex_settings"}
        if not required.issubset(tables):
            return False, f"Tabel wajib kurang: {', '.join(sorted(required - tables))}"
        # Basic integrity check.
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if str(integrity).lower() != "ok":
            return False, f"SQLite integrity_check: {integrity}"
        return True, "OK"
    finally:
        conn.close()


def restore_database_from_file(source_path):
    """Safely replace the live DB after validation and create a rollback backup first."""
    global IS_BOT_PAUSED
    ok, detail = validate_restore_db(source_path)
    if not ok:
        return False, detail

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rollback = BACKUP_DIR / f"before_restore_{ts}.db"
    try:
        # Backup current live DB first.
        src = sqlite3.connect(DB_FILE, timeout=20)
        try:
            try: src.execute("PRAGMA wal_checkpoint(FULL)")
            except Exception: pass
            dst = sqlite3.connect(str(rollback), timeout=20)
            try: src.backup(dst)
            finally: dst.close()
        finally:
            src.close()

        IS_BOT_PAUSED = True
        time.sleep(1.0)
        # Remove stale WAL/SHM sidecars before atomic replacement.
        for suffix in ("-wal", "-shm"):
            sidecar = Path(DB_FILE + suffix)
            try: sidecar.unlink()
            except FileNotFoundError: pass
        os.replace(str(source_path), DB_FILE)
        init_db()
        load_settings()
        reload_exchanges()
        return True, str(rollback)
    except Exception as e:
        return False, str(e)


@dp.callback_query(F.data == "backup_menu")
async def cb_backup_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 BUAT BACKUP", callback_data="backup_create")],
        [InlineKeyboardButton(text="♻️ RESTORE BACKUP", callback_data="backup_restore")],
        [InlineKeyboardButton(text="⬅️ KEMBALI", callback_data="menu")]
    ])
    await callback.message.edit_text(
        "💾 <b>BACKUP / RESTORE</b>\n\n"
        "Backup berisi database bot: setting, konfigurasi CEX, history, pending/open state.\n"
        "Source code/config tetap perlu disimpan dari Git/VPS secara terpisah.\n\n"
        "⚠️ Saat migrasi, jalankan hanya satu instance bot.",
        reply_markup=kb
    )


@dp.callback_query(F.data == "backup_create")
async def cb_backup_create(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback, "Membuat backup...")
    try:
        path = await asyncio.to_thread(create_database_backup)
        sent = await bot.send_document(
            chat_id=callback.message.chat.id,
            document=FSInputFile(str(path)),
            caption=(
                "💾 <b>BACKUP BERHASIL</b>\n\n"
                "Simpan file ini untuk migrasi VPS/restore.\n"
                "Database berisi konfigurasi CEX dan history trading.\n\n"
                "🗑️ Pesan backup ini akan otomatis dihapus dalam <b>5 menit</b>.\n"
                "💾 File backup tetap tersimpan di VPS."
            )
        )
        asyncio.create_task(delete_backup_message_later(
            callback.message.chat.id, sent.message_id, 300
        ))
    except Exception as e:
        await callback.message.answer(f"❌ Backup gagal: <code>{html.escape(str(e))}</code>")


@dp.callback_query(F.data == "backup_restore")
async def cb_backup_restore(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    PENDING_RESTORE.add(callback.message.chat.id)
    await safe_answer(callback)
    await callback.message.edit_text(
        "♻️ <b>RESTORE BACKUP</b>\n\n"
        "Kirim file <code>.db</code> backup Alchemist sekarang.\n\n"
        "⚠️ Database aktif akan diganti. Bot akan otomatis PAUSE setelah restore.\n"
        "Sebelum mengganti, database saat ini dibuatkan rollback backup.\n\n"
        "Kirim hanya backup yang kamu percaya.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ BATAL", callback_data="restore_cancel")]
        ])
    )


@dp.callback_query(F.data == "restore_cancel")
async def cb_restore_cancel(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    PENDING_RESTORE.discard(callback.message.chat.id)
    await safe_answer(callback, "Restore dibatalkan")
    await callback.message.edit_text("🤖 <b>ALCHEMIST v2</b>\n\nPilih menu:", reply_markup=main_keyboard())


@dp.message(F.document)
async def handle_restore_document(message: Message):
    if not is_allowed_chat(message): return
    chat_id = message.chat.id
    if chat_id not in PENDING_RESTORE:
        return
    doc = message.document
    name = (doc.file_name or "").lower()
    if not (name.endswith(".db") or name.endswith(".zip")):
        await message.answer("❌ Kirim file backup <code>.zip</code> atau database SQLite <code>.db</code> saja.")
        return

    PENDING_RESTORE.discard(chat_id)
    temp_path = BACKUP_DIR / f"incoming_restore_{int(time.time())}.db"
    downloaded_path = BACKUP_DIR / f"incoming_{int(time.time())}{Path(name).suffix}"
    status = await message.answer("♻️ <b>Memeriksa backup...</b>")
    try:
        tg_file = await bot.get_file(doc.file_id)
        await bot.download_file(tg_file.file_path, destination=str(downloaded_path))
        if name.endswith(".zip"):
            with zipfile.ZipFile(downloaded_path, "r") as z:
                members = [m for m in z.namelist() if not m.endswith("/")]
                db_members = [m for m in members if Path(m).name.lower() == "trades.db"]
                if not db_members:
                    raise ValueError("Backup ZIP tidak berisi trades.db")
                member = db_members[0]
                if Path(member).is_absolute() or ".." in Path(member).parts:
                    raise ValueError("Path trades.db di ZIP tidak aman")
                with z.open(member) as src, open(temp_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)
        else:
            shutil.copy2(downloaded_path, temp_path)
        ok, detail = await asyncio.to_thread(restore_database_from_file, temp_path)
        if ok:
            await status.edit_text(
                "✅ <b>RESTORE BERHASIL</b>\n\n"
                "Database sudah diganti dan bot sekarang <b>PAUSED</b>.\n"
                "Cek <b>RUNTIME STATUS</b> / posisi terlebih dahulu sebelum RESUME.\n\n"
                f"Rollback otomatis dibuat: <code>{html.escape(str(detail))}</code>",
                reply_markup=main_keyboard()
            )
        else:
            await status.edit_text(f"❌ <b>RESTORE GAGAL</b>\n\n<code>{html.escape(str(detail))}</code>", reply_markup=main_keyboard())
    except Exception as e:
        await status.edit_text(f"❌ <b>RESTORE GAGAL</b>\n\n<code>{html.escape(str(e))}</code>", reply_markup=main_keyboard())
    finally:
        for cleanup_path in (temp_path, downloaded_path):
            try: cleanup_path.unlink()
            except Exception: pass


@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    await send_main_menu(message, delete_trigger=True)


@dp.message(Command("start"))
@dp.message(Command("help"))
async def cmd_start(message: Message):
    await send_main_menu(message, delete_trigger=False)


@dp.message(Command("backup"))
async def cmd_backup(message: Message):
    if not is_allowed_chat(message): return
    try:
        await message.delete()
    except Exception:
        pass
    try:
        path = await asyncio.to_thread(create_database_backup)
        sent = await bot.send_document(
            chat_id=message.chat.id,
            document=FSInputFile(str(path)),
            caption=(
                "💾 <b>BACKUP BERHASIL</b>\n"
                "Simpan file ini untuk migrasi VPS/restore.\n\n"
                "🗑️ Pesan backup ini akan otomatis dihapus dalam <b>5 menit</b>.\n"
                "💾 File backup tetap tersimpan di VPS."
            )
        )
        asyncio.create_task(delete_backup_message_later(
            message.chat.id, sent.message_id, 300
        ))
    except Exception as e:
        await bot.send_message(message.chat.id, f"❌ Backup gagal: <code>{html.escape(str(e))}</code>")


@dp.message(Command("restore"))
async def cmd_restore(message: Message):
    if not is_allowed_chat(message): return
    PENDING_RESTORE.add(message.chat.id)
    try: await message.delete()
    except Exception: pass
    await bot.send_message(
        message.chat.id,
        "♻️ <b>RESTORE BACKUP</b>\n\nKirim file <code>.db</code> backup Alchemist sekarang.\n\n⚠️ Database aktif akan diganti dan bot akan PAUSED setelah restore.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ BATAL", callback_data="restore_cancel")]])
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
    await message.answer(build_positions_text(), reply_markup=positions_keyboard())


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
    await message.answer(build_positions_text(), reply_markup=main_keyboard())


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
        "🔐 <b>SET API</b>\n\nPilih CEX dulu. Setelah itu bot akan meminta <b>API Key</b> lalu <b>Secret Key</b> secara terpisah.",
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
        "🔑 <b>PILIH CEX</b>\n\n"
        "Bot akan meminta API Key → Secret Key."
        " Untuk BITGET, setelah itu bot meminta API Passphrase.",
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
        f"🔑 <b>SET API — {ex_id.upper()}</b>\n\n"
        "Kirim <b>API Key</b> saja di pesan berikutnya.\n"
        "Bot akan langsung meminta Secret Key setelahnya.\n\n"
        "⚠️ Jangan kirim API key ke chat lain.",
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
        await safe_answer(
            callback,
            "Set API + Passphrase Bitget dulu" if ex_id == 'bitget' else "Set API dulu sebelum ON",
            True
        )
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


@dp.message(F.text)
async def handle_api_setup_message(message: Message):
    if not is_allowed_chat(message):
        return

    chat_id = message.chat.id
    state = PENDING_API_SETUP.get(chat_id)
    if not state:
        return

    text = (message.text or '').strip()
    if not text:
        return

    ex_id = state['ex_id']

    # Delete the user's credential message immediately.
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message.message_id)
    except Exception:
        pass

    if state['stage'] == 'api_key':
        state['stage'] = 'secret'
        state['api'] = text
        ack = await bot.send_message(
            chat_id,
            f"🔐 <b>API Key diterima</b> untuk {ex_id.upper()}.\n"
            "🗑️ Pesan API Key sudah dihapus.\n\n"
            "Sekarang kirim <b>Secret Key</b> saja. "
            "Pesan Secret Key juga akan dihapus otomatis.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ BATAL & HAPUS SESI", callback_data="api_cancel")]
            ])
        )
        state['ack_message_id'] = ack.message_id
        return

    if state['stage'] == 'secret':
        state['stage'] = 'passphrase' if ex_id == 'bitget' else 'validate'
        state['secret'] = text

        if ex_id == 'bitget':
            ack = await bot.send_message(
                chat_id,
                "🔐 <b>Secret Key diterima</b> untuk BITGET.\n"
                "🗑️ Pesan Secret Key sudah dihapus.\n\n"
                "Sekarang kirim <b>API Passphrase</b> Bitget. "
                "BUKAN password login.\n"
                "Pesan Passphrase juga akan dihapus otomatis.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ BATAL & HAPUS SESI", callback_data="api_cancel")]
                ])
            )
            state['ack_message_id'] = ack.message_id
            return

    # Final credential is the secret for non-Bitget, or passphrase for Bitget.
    api = state.get('api', '')
    secret = state.get('secret', '')
    password = text if ex_id == 'bitget' else ''
    PENDING_API_SETUP.pop(chat_id, None)

    for mid in (state.get('ack_message_id'), state.get('prompt_message_id')):
        if mid:
            try:
                await bot.delete_message(chat_id, mid)
            except Exception:
                pass

    status_msg = await bot.send_message(
        chat_id, f"🔄 <b>Memvalidasi API {ex_id.upper()}...</b>"
    )

    ok, detail, account_details = await asyncio.to_thread(
        validate_cex_api, ex_id, api, secret, password
    )

    # Do not keep credentials in this handler state after validation.
    api = ''
    secret = ''
    password = ''

    if not ok:
        safe_detail = html.escape(detail)
        await status_msg.edit_text(
            f"❌ <b>API {ex_id.upper()} TIDAK VALID</b>\n\n"
            f"Alasan: <code>{safe_detail}</code>\n\n"
            "API <b>tidak disimpan</b>.",
            reply_markup=api_menu_keyboard()
        )
        return

    username = account_details.get('username', '-')
    uid = account_details.get('uid', '-')
    alias = account_details.get('alias', '-')
    label_parts = []
    if username != '-':
        label_parts.append(username)
    if uid != '-':
        label_parts.append(f"UID {uid}")
    if alias != '-':
        label_parts.append(alias)
    account_label = " | ".join(label_parts) if label_parts else "Private account terverifikasi"

    stored_info = {
        'Exchange': account_details.get('exchange', '-'),
        'Account Type': account_details.get('account_type', '-'),
        'Market': account_details.get('market_type', '-'),
        'Private API': account_details.get('private_api', '-'),
        'USDT Total': account_details.get('total_usdt'),
        'USDT Free': account_details.get('free_usdt'),
        'USDT Used': account_details.get('used_usdt'),
        'Open Positions': account_details.get('open_positions', 'N/A'),
        'Open Orders': account_details.get('open_orders', 'N/A'),
        'Trade Permission': account_details.get('trade_permission', '-'),
        'Read Only': account_details.get('read_only', '-')
    }
    stored_info = {
        k: v for k, v in stored_info.items()
        if v not in (None, '', '-')
    }

    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.cursor().execute(
        "UPDATE cex_settings "
        "SET api_key=?, secret=?, password=?, account_label=?, account_info=? "
        "WHERE ex_id=?",
        (
            state.get('api', ''),
            state.get('secret', ''),
            text if ex_id == 'bitget' else '',
            account_label,
            json.dumps(stored_info, ensure_ascii=False),
            ex_id
        )
    )
    conn.commit()
    conn.close()

    reload_exchanges()

    await status_msg.edit_text(
        f"✅ <b>API {ex_id.upper()} VALID</b>\n\n"
        "Private endpoint berhasil diakses.\n\n"
        f"{format_account_details(account_details)}\n\n"
        "🔗 API sudah disimpan. CEX masih <b>OFF</b> sampai kamu menekan tombol ON.",
        reply_markup=cex_control_keyboard()
    )


@dp.callback_query(F.data == "menu")
async def cb_menu(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    try:
        await callback.message.edit_text("🤖 <b>ALCHEMIST v2</b>\n\nPilih menu:", reply_markup=main_keyboard())
        LAST_MENU_MESSAGE[callback.message.chat.id] = callback.message.message_id
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "positions")
async def cb_positions(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    await safe_answer(callback)
    try:
        await callback.message.edit_text(build_positions_text(), reply_markup=positions_keyboard())
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


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


@dp.callback_query(F.data == "runtime_status")
async def cb_runtime_status(callback: CallbackQuery):
    if not is_allowed_chat(callback):
        return
    await safe_answer(callback)
    await callback.message.edit_text(build_runtime_status_text(), reply_markup=runtime_status_keyboard())


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
    global IS_BOT_PAUSED
    if not is_allowed_chat(callback):
        return

    value = not CONFIG.get('DAILY_LOSS_LIMIT', True)
    CONFIG['DAILY_LOSS_LIMIT'] = value
    update_db_setting('daily_loss_limit', 1.0 if value else 0.0)

    # If the limit is disabled, allow the engine to continue after a previous
    # automatic daily-loss pause.
    if not value and IS_BOT_PAUSED:
        IS_BOT_PAUSED = False
        notify_runtime_status(
            "RESUMED",
            "▶️ <b>TRADING RESUMED</b>\nDaily Loss Limit dimatikan."
        )

    await safe_answer(
        callback,
        "Daily Loss Limit ON" if value else "Daily Loss Limit OFF"
    )
    await callback.message.edit_text(
        "🛑 <b>DAILY LOSS LIMIT</b>\n\n"
        f"Status: {'🟢 ON' if value else '🔴 OFF'}\n"
        f"Batas: {MAX_DAILY_SL} SL/hari\n\n"
        + (
            "Jika ON, bot pause setelah batas SL harian tercapai."
            if value else
            "Jika OFF, SL tetap dicatat tetapi tidak mem-pause bot."
        ),
        reply_markup=bot_control_keyboard()
    )


@dp.callback_query(F.data == "cancelall")
async def cb_cancelall(callback: CallbackQuery):
    if not is_allowed_chat(callback): return
    cancel_all_pending_orders_on_pause()
    await safe_answer(callback, "Pending order diproses")
    await callback.message.edit_text(
        build_positions_text(),
        reply_markup=positions_keyboard()
    )


@dp.callback_query(F.data.startswith("closepos:"))
async def cb_close_position(callback: CallbackQuery):
    if not is_allowed_chat(callback):
        return

    try:
        trade_id = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await safe_answer(callback, "ID posisi tidak valid.", show_alert=True)
        return

    ok, detail = await asyncio.to_thread(close_open_trade, trade_id)
    if ok:
        await safe_answer(callback, "Posisi ditutup.")
        send_telegram(f"🚨 <b>MANUAL CLOSE</b>\n{html.escape(detail)}")
    else:
        await safe_answer(callback, detail[:180], show_alert=True)

    try:
        await callback.message.edit_text(
            build_positions_text(),
            reply_markup=positions_keyboard()
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


@dp.callback_query(F.data == "closeallopen")
async def cb_close_all_open(callback: CallbackQuery):
    if not is_allowed_chat(callback):
        return

    results = await asyncio.to_thread(close_all_open_trades)
    success = sum(1 for ok, _ in results if ok)
    failed = [detail for ok, detail in results if not ok]

    if not results:
        await safe_answer(callback, "Tidak ada posisi OPEN.", show_alert=True)
    elif failed:
        await safe_answer(callback, f"Berhasil {success}, gagal {len(failed)}.", show_alert=True)
    else:
        await safe_answer(callback, f"{success} posisi ditutup.")

    if results:
        lines = [f"• {detail}" for _, detail in results]
        send_telegram("🚨 <b>EMERGENCY CLOSE ALL OPEN</b>\n" + "\n".join(lines))

    try:
        await callback.message.edit_text(
            build_positions_text(),
            reply_markup=positions_keyboard()
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


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

@app.route('/api/cex-monitor')
def api_cex_monitor():
    """Return live CEX configuration/connection state for the web dashboard.

    This endpoint only reads local bot state/database metadata. It does not
    call private exchange endpoints on every dashboard refresh.
    """
    try:
        rows = db_read(
            "SELECT ex_id, api_key, enabled, account_label, account_info "
            "FROM cex_settings ORDER BY ex_id"
        )

        items = []
        for ex_id, api_key, enabled, account_label, account_info in rows:
            info = {}
            if account_info:
                try:
                    info = json.loads(account_info)
                except Exception:
                    info = {}

            configured = bool(api_key)
            loaded = ex_id in exchanges
            enabled = bool(enabled)

            if not enabled:
                state = 'OFF'
                state_icon = '🔴'
            elif not configured:
                state = 'API NOT SET'
                state_icon = '🟠'
            elif loaded:
                state = 'ON / READY'
                state_icon = '🟢'
            else:
                state = 'ON / NOT LOADED'
                state_icon = '🟡'

            items.append({
                'id': ex_id,
                'name': ex_id.upper(),
                'enabled': enabled,
                'configured': configured,
                'loaded': loaded,
                'state': state,
                'state_icon': state_icon,
                'leverage': CONFIG['LEVERAGE'].get(ex_id, 50),
                'account': account_label or 'Belum diverifikasi',
                'account_type': info.get('Account Type', '-'),
                'market': info.get('Market', '-'),
                'private_api': info.get('Private API', '-'),
                'usdt_free': info.get('USDT Free'),
                'usdt_total': info.get('USDT Total'),
                'open_positions': info.get('Open Positions', 'N/A'),
                'open_orders': info.get('Open Orders', 'N/A'),
                'trade_permission': info.get('Trade Permission', '-'),
                'read_only': info.get('Read Only', '-')
            })

        return jsonify({
            'ok': True,
            'bot_paused': bool(IS_BOT_PAUSED),
            'dry_run': bool(CONFIG.get('DRY_RUN', True)),
            'scanning': bool(exchanges) and not IS_BOT_PAUSED and is_kill_zone(),
            'last_runtime_status': last_runtime_status or 'UNKNOWN',
            'updated': datetime.now(WIB).strftime('%d/%m/%Y, %H:%M:%S'),
            'cex': items
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'cex': []}), 500


@app.route('/api/stats')
def api_stats():
    """Dashboard statistics from the same SQLite DB used by the bot."""
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        df = pd.read_sql_query(
            "SELECT id, symbol, side, status, created_at "
            "FROM trades WHERE status IN ('CLOSED (TP)', 'CLOSED (SL)') "
            "ORDER BY id ASC",
            conn
        )
        conn.close()

        if df.empty:
            return jsonify({
                'ok': True,
                'dates': [], 'pnl': [],
                'win_rate': 0, 'wins': 0, 'losses': 0,
                'total_trades': 0, 'profit_factor': 0,
                'net_pnl': 0, 'avg_win': 0, 'avg_loss': 0,
                'max_drawdown': 0, 'best_trade': 0, 'worst_trade': 0,
                'recent': [], 'daily_pnl': {}, 'daily_trades': {}
            })

        margin = float(CONFIG.get('MARGIN_PER_TRADE', 2.0))
        df['pnl_value'] = df['status'].apply(
            lambda x: margin if x == 'CLOSED (TP)' else -margin
        )
        df['cum_pnl'] = df['pnl_value'].cumsum()
        running_peak = df['cum_pnl'].cummax()
        drawdown = running_peak - df['cum_pnl']
        max_drawdown = float(drawdown.max()) if not drawdown.empty else 0.0

        dt = pd.to_datetime(df['created_at'], errors='coerce')
        df['date_only'] = dt.dt.strftime('%d %b %H:%M')
        df['day_key'] = dt.dt.strftime('%Y-%m-%d')

        wins = int((df['status'] == 'CLOSED (TP)').sum())
        losses = int((df['status'] == 'CLOSED (SL)').sum())
        total = wins + losses
        win_rate = round((wins / total) * 100, 1) if total else 0.0

        gross_profit = float(df.loc[df['pnl_value'] > 0, 'pnl_value'].sum())
        gross_loss = abs(float(df.loc[df['pnl_value'] < 0, 'pnl_value'].sum()))
        profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
        avg_win = round(gross_profit / wins, 2) if wins else 0.0
        avg_loss = round(gross_loss / losses, 2) if losses else 0.0
        net_pnl = round(float(df['pnl_value'].sum()), 2)
        best_trade = round(float(df['pnl_value'].max()), 2)
        worst_trade = round(float(df['pnl_value'].min()), 2)

        daily = (
            df.dropna(subset=['day_key'])
              .groupby('day_key', as_index=False)['pnl_value']
              .sum()
        )
        daily_pnl = {
            str(row['day_key']): round(float(row['pnl_value']), 2)
            for _, row in daily.iterrows()
        }
        daily_trades = (
            df.dropna(subset=['day_key'])
              .groupby('day_key')
              .size()
              .to_dict()
        )
        daily_trades = {str(k): int(v) for k, v in daily_trades.items()}

        recent = df.tail(10)[::-1].to_dict('records')
        return jsonify({
            'ok': True,
            'dates': df['date_only'].tolist(),
            'pnl': df['cum_pnl'].tolist(),
            'win_rate': win_rate,
            'wins': wins,
            'losses': losses,
            'total_trades': total,
            'profit_factor': profit_factor,
            'net_pnl': net_pnl,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'max_drawdown': round(max_drawdown, 2),
            'best_trade': best_trade,
            'worst_trade': worst_trade,
            'recent': recent,
            'daily_pnl': daily_pnl,
            'daily_trades': daily_trades
        })
    except Exception as e:
        return jsonify({
            'ok': False,
            'error': str(e),
            'dates': [], 'pnl': [], 'win_rate': 0, 'wins': 0, 'losses': 0,
            'total_trades': 0, 'profit_factor': 0, 'net_pnl': 0,
            'avg_win': 0, 'avg_loss': 0, 'max_drawdown': 0,
            'best_trade': 0, 'worst_trade': 0, 'recent': [],
            'daily_pnl': {}, 'daily_trades': {}
        }), 500


@app.route('/api/trade-detail')
def api_trade_detail():
    """Return the latest 30 trades for the dashboard."""
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        rows = conn.execute(
            "SELECT id, exchange_id, symbol, side, entry_price, sl_price, "
            "tp_price, order_id, bars_count, status, created_at "
            "FROM trades ORDER BY id DESC LIMIT 30"
        ).fetchall()
        conn.close()
        cols = [
            'id', 'exchange', 'symbol', 'side', 'entry', 'sl', 'tp',
            'order_id', 'bars', 'status', 'created_at'
        ]
        return jsonify({'ok': True, 'trades': [dict(zip(cols, row)) for row in rows]})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e), 'trades': []}), 500


@app.route('/api/engine-status')
def api_engine_status():
    """Live engine state for the web dashboard."""
    try:
        return jsonify({
            'ok': True,
            'status': last_runtime_status or ('PAUSED' if IS_BOT_PAUSED else 'RUNNING'),
            'paused': bool(IS_BOT_PAUSED),
            'dry_run': bool(CONFIG.get('DRY_RUN', True)),
            'kill_zone': bool(is_kill_zone()),
            'active_cex': len(exchanges),
            'daily_sl': int(daily_sl_count),
            'max_daily_sl': int(MAX_DAILY_SL),
            'open_positions': db_read("SELECT COUNT(*) FROM trades WHERE status='OPEN'")[0][0],
            'pending_orders': db_read("SELECT COUNT(*) FROM trades WHERE status='PENDING'")[0][0],
            'uptime_seconds': int(time.time() - BOT_STARTED_AT),
            'last_event': LAST_EVENT_TEXT,
            'exchange_runtime': EXCHANGE_RUNTIME,
            'updated': datetime.now(WIB).strftime('%d/%m/%Y %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/errors')
def api_errors():
    """Recent runtime errors for the dashboard Error Center."""
    return jsonify({'ok': True, 'errors': list(reversed(ERROR_HISTORY[-20:]))})

def notify_runtime_status(new_status, text):
    global last_runtime_status
    set_runtime_event(new_status)
    if last_runtime_status != new_status:
        last_runtime_status = new_status
        send_telegram(text)


def main_loop():
    global daily_sl_count, IS_BOT_PAUSED

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
            _submit_async(update_public_signal_outcomes())
            if now.hour == 7 and now.minute == 0 and now.second < 15:
                _submit_async(send_public_daily_report())
                if now.weekday() == 6:
                    _submit_async(_public_weekly_report())

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
                    set_runtime_event('Scanning selesai / menunggu candle berikutnya')
            else:
                notify_runtime_status(
                    'WAITING',
                    "⏹️ <b>STOP TRADE / WAITING</b>\nDi luar kill zone. Bot tidak membuka trade baru dan menunggu sesi berikutnya."
                )

        except Exception as e:
            send_error_alert(str(e))

        time.sleep(10)


async def app_main():
    global MAIN_ASYNC_LOOP
    MAIN_ASYNC_LOOP = asyncio.get_running_loop()
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
