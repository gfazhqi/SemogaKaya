import os
import sys
import time
import sqlite3
import threading
import pandas as pd
import requests
import ccxt
from datetime import datetime, timedelta, timezone
from flask import Flask

# =================================================================
# CONFIGURATION & PARAMETERS
# =================================================================
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"  # Otomatis jadi kunci pengaman bot

MARGIN_PER_TRADE = 2.0         # USDT Margin per posisi crypto
LEVERAGE = 50                  # Leverage Futures
SL_BUFFER_PERCENT = 0.0005     # Buffer Stop Loss (0.05%)
MAX_DAILY_SL = 2               # Batas maksimal SL per hari sebelum auto-pause
MAX_CONCURRENT_POSITIONS = 2    # Hard Cap: Maksimal 2 posisi crypto aktif bersamaan

DB_FILE = "trades.db"
WIB = timezone(timedelta(hours=7))

# Whitelist Pair Manual Alert (FX & Komoditas Khusus)
MANUAL_ALERT_PAIRS = ['XAU/USDT', 'EUR/USDT', 'GBP/USDT', 'USOIL/USDT', 'WTI/USDT', 'USO/USDT']

# =================================================================
# MULTI-CEX ARCHITECTURE ENGINE
# =================================================================
EXCHANGE_CONFIGS = {
    'bingx': {
        'enabled': True,   # UTAMA: BingX Aktif
        'apiKey': 'YOUR_BINGX_API_KEY',
        'secret': 'YOUR_BINGX_SECRET_KEY',
        'options': {'defaultType': 'swap'},
        'enableRateLimit': True
    },
    'bitget': {
        'enabled': False,  # Siap diaktifkan nanti (ganti True)
        'apiKey': 'YOUR_BITGET_API_KEY',
        'secret': 'YOUR_BITGET_SECRET_KEY',
        'options': {'defaultType': 'swap'},
        'enableRateLimit': True
    },
    'bybit': {
        'enabled': False,  # Siap diaktifkan nanti (ganti True)
        'apiKey': 'YOUR_BYBIT_API_KEY',
        'secret': 'YOUR_BYBIT_SECRET_KEY',
        'options': {'defaultType': 'linear'},
        'enableRateLimit': True
    },
    'binance': {
        'enabled': False,  # Siap diaktifkan nanti (ganti True)
        'apiKey': 'YOUR_BINANCE_API_KEY',
        'secret': 'YOUR_BINANCE_SECRET_KEY',
        'options': {'defaultType': 'future'},
        'enableRateLimit': True
    }
}

# Initialize Enabled Exchanges
exchanges = {}
for ex_id, cfg in EXCHANGE_CONFIGS.items():
    if cfg['enabled']:
        ex_class = getattr(ccxt, ex_id)
        exchanges[ex_id] = ex_class({
            'apiKey': cfg['apiKey'],
            'secret': cfg['secret'],
            'options': cfg['options'],
            'enableRateLimit': cfg['enableRateLimit']
        })

# Global Runtime Variables
IS_BOT_PAUSED = False
daily_sl_count = 0
last_error_time = 0
ERROR_COOLDOWN = 300  # Cooldown alert error (5 menit)

# =================================================================
# DATABASE INITIALIZATION (SQLite Persistence)
# =================================================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
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
        )
    ''')
    conn.commit()
    conn.close()

# =================================================================
# TELEGRAM NOTIFICATION & ERROR GUARD
# =================================================================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Error Telegram Send: {e}")

def send_error_alert(err_msg):
    global last_error_time
    now = time.time()
    if now - last_error_time > ERROR_COOLDOWN:
        send_telegram(f"⚠️ *[ERROR ALERT GUARD]*\n\n`{err_msg}`")
        last_error_time = now

# =================================================================
# HELPER & FILTER FUNCTIONS
# =================================================================
def is_kill_zone():
    """Validasi Sesi Aktif Kill Zone WIB (London: 14-17 WIB, NY: 19-22 WIB)"""
    now_wib = datetime.now(WIB)
    hour = now_wib.hour
    return (14 <= hour < 17) or (19 <= hour < 22)

def is_manual_alert_symbol(symbol):
    """Mengecek apakah pair termasuk ke dalam Whitelist Manual Alert (XAU, EURUSD, GBPUSD, USOIL)"""
    clean_symbol = symbol.split(':')[0].upper()
    return any(p in clean_symbol for p in ['XAU', 'EUR/USDT', 'GBP/USDT', 'USOIL', 'WTI', 'USO'])

def get_free_usdt_balance(ex_instance):
    try:
        balance = ex_instance.fetch_balance()
        return float(balance.get('USDT', {}).get('free', 0.0))
    except Exception as e:
        send_error_alert(f"Gagal mengambil saldo bursa: {e}")
        return 0.0

def cancel_all_pending_orders_on_pause():
    """Membatalkan seluruh sisa pending limit order di semua bursa aktif"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT id, exchange_id, symbol, order_id FROM trades WHERE status='PENDING'")
        rows = c.fetchall()

        for row in rows:
            trade_id, ex_id, symbol, order_id = row
            if ex_id in exchanges:
                try:
                    exchanges[ex_id].cancel_order(order_id, symbol)
                    c.execute("UPDATE trades SET status='CANCELLED (Manual/Auto-Pause)' WHERE id=?", (trade_id,))
                except Exception as e:
                    print(f"Error cancel order {symbol} on {ex_id}: {e}")

        conn.commit()
        conn.close()
        send_telegram("🧹 *[CLEANUP]* Semua sisa pending limit order crypto telah dibatalkan!")
    except Exception as e:
        print(f"Error in cancel_all_pending_orders_on_pause: {e}")

def check_and_update_daily_sl():
    """Mengecek batas maksimal SL harian dan mengaktifkan Auto-Pause + Auto-Cancel"""
    global daily_sl_count, IS_BOT_PAUSED
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) FROM trades 
            WHERE created_at >= date('now', 'start of day') 
            AND status LIKE '%SL%'
        """)
        daily_sl_count = c.fetchone()[0]
        conn.close()

        if daily_sl_count >= MAX_DAILY_SL and not IS_BOT_PAUSED:
            IS_BOT_PAUSED = True
            cancel_all_pending_orders_on_pause()
            msg = (
                f"🛑 *[DAILY LOSS LIMIT REACHED]*\n\n"
                f"Hari ini udah kena `{daily_sl_count}x SL`.\n"
                f"Bot di-*PAUSE* & sisa limit order dibatalkan otomatis.\n"
                f"Scanner bakal aktif lagi besok jam 07:00 WIB!"
            )
            send_telegram(msg)
    except Exception as e:
        print(f"Error check daily SL: {e}")

# =================================================================
# SMC STRATEGY LOGIC ENGINE
# =================================================================
def analyze_smc_retrace(df_m15, df_h1, daily_open):
    ema50_h1 = df_h1['close'].ewm(span=50, adjust=False).mean().iloc[-2]
    h1_close = df_h1['close'].iloc[-2]
    c_prev = df_m15.iloc[-2]
    
    signal = None
    entry_price, sl_price, tp_price = 0.0, 0.0, 0.0

    # Bullish Setup
    if h1_close > ema50_h1 and c_prev['close'] > daily_open:
        min_low_recent = df_m15['low'].iloc[-7:-2].min()
        if c_prev['low'] < min_low_recent and c_prev['close'] > c_prev['open']:
            low_p = c_prev['low']
            high_p = c_prev['high']
            entry_price = high_p - (0.5 * (high_p - low_p))
            sl_price = low_p * (1.0 - SL_BUFFER_PERCENT)
            risk = entry_price - sl_price
            tp_price = entry_price + (1.0 * risk)
            signal = 'BUY'

    # Bearish Setup
    elif h1_close < ema50_h1 and c_prev['close'] < daily_open:
        max_high_recent = df_m15['high'].iloc[-7:-2].max()
        if c_prev['high'] > max_high_recent and c_prev['close'] < c_prev['open']:
            high_p = c_prev['high']
            low_p = c_prev['low']
            entry_price = low_p + (0.5 * (high_p - low_p))
            sl_price = high_p * (1.0 + SL_BUFFER_PERCENT)
            risk = sl_price - entry_price
            tp_price = entry_price - (1.0 * risk)
            signal = 'SELL'

    return signal, entry_price, sl_price, tp_price

# =================================================================
# ORDER TRACKING & POSITION LIFECYCLE MONITORING
# =================================================================
def update_orders_and_positions():
    """Memantau status pending order (Filled/Cancel) & posisi aktif (TP/SL Hit)"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, exchange_id, symbol, side, entry_price, sl_price, tp_price, order_id, bars_count, status FROM trades WHERE status IN ('PENDING', 'OPEN')")
    rows = c.fetchall()

    for row in rows:
        trade_id, ex_id, symbol, side, entry, sl, tp, order_id, bars, status = row
        if ex_id not in exchanges:
            continue
        try:
            ex_instance = exchanges[ex_id]
            ticker = ex_instance.fetch_ticker(symbol)
            curr_price = ticker['last']
            new_bars = bars + 1

            # 1. MONITORING PENDING ORDER
            if status == 'PENDING':
                new_status = 'PENDING'
                
                # Cek apakah Limit Order terisi (Filled)
                if (side == 'BUY' and curr_price <= entry) or (side == 'SELL' and curr_price >= entry):
                    new_status = 'OPEN'
                    send_telegram(
                        f"🔔 *[ORDER FILLED - LIVE POSITION]*\n\n"
                        f"Exchange: `{ex_id.upper()}`\n"
                        f"Pair: `{symbol}`\n"
                        f"Side: `{side}`\n"
                        f"Entry: `{entry:.5f}`\n"
                        f"Posisi resmi *AKTIF* di bursa!"
                    )
                # Auto-Cancel Checks
                elif side == 'BUY':
                    if curr_price >= tp:
                        new_status = 'MISSED (TP First)'
                    elif curr_price <= sl:
                        new_status = 'CANCELLED (SL First)'
                    elif new_bars > 30:
                        new_status = 'INVALID (>30 Bars)'
                elif side == 'SELL':
                    if curr_price <= tp:
                        new_status = 'MISSED (TP First)'
                    elif curr_price >= sl:
                        new_status = 'CANCELLED (SL First)'
                    elif new_bars > 30:
                        new_status = 'INVALID (>30 Bars)'

                if new_status != 'PENDING':
                    if 'CANCELLED' in new_status or 'MISSED' in new_status or 'INVALID' in new_status:
                        try:
                            ex_instance.cancel_order(order_id, symbol)
                        except Exception:
                            pass
                        send_telegram(
                            f"⚪ *[AUTO-CANCEL ORDER]*\n\n"
                            f"Exchange: `{ex_id.upper()}`\n"
                            f"Pair: `{symbol}`\n"
                            f"Side: `{side}`\n"
                            f"Alasan: `{new_status}`"
                        )
                    c.execute("UPDATE trades SET status=?, bars_count=? WHERE id=?", (new_status, new_bars, trade_id))
                else:
                    c.execute("UPDATE trades SET bars_count=? WHERE id=?", (new_bars, trade_id))

            # 2. MONITORING POSISI LIVE (OPEN) FOR TP / SL HIT
            elif status == 'OPEN':
                if side == 'BUY':
                    if curr_price >= tp:
                        c.execute("UPDATE trades SET status='CLOSED (TP)' WHERE id=?", (trade_id,))
                        send_telegram(f"🎉 *[TAKE PROFIT HIT]*\n\nPair: `{symbol}`\nSide: `BUY`\nProfit: `+1R` 🚀")
                    elif curr_price <= sl:
                        c.execute("UPDATE trades SET status='CLOSED (SL)' WHERE id=?", (trade_id,))
                        send_telegram(f"❌ *[STOP LOSS HIT]*\n\nPair: `{symbol}`\nSide: `BUY`\nLoss: `-1R` 🔻")
                elif side == 'SELL':
                    if curr_price <= tp:
                        c.execute("UPDATE trades SET status='CLOSED (TP)' WHERE id=?", (trade_id,))
                        send_telegram(f"🎉 *[TAKE PROFIT HIT]*\n\nPair: `{symbol}`\nSide: `SELL`\nProfit: `+1R` 🚀")
                    elif curr_price >= sl:
                        c.execute("UPDATE trades SET status='CLOSED (SL)' WHERE id=?", (trade_id,))
                        send_telegram(f"❌ *[STOP LOSS HIT]*\n\nPair: `{symbol}`\nSide: `SELL`\nLoss: `-1R` 🔻")

        except Exception as e:
            print(f"Error update orders/positions {symbol} on {ex_id}: {e}")

    conn.commit()
    conn.close()

# =================================================================
# MARKET SCANNER ENGINE
# =================================================================
def scan_markets():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM trades WHERE status IN ('PENDING', 'OPEN')")
    active_crypto_count = c.fetchone()[0]
    conn.close()

    for ex_id, ex_instance in exchanges.items():
        try:
            tickers = ex_instance.fetch_tickers()
            all_symbols = [symbol for symbol in tickers.keys() if symbol.endswith('/USDT:USDT') or symbol.endswith('/USDT')][:60]

            for symbol in all_symbols:
                # 1. KHUSUS FX & KOMODITAS (XAU, EURUSD, GBPUSD, USOIL -> ALERT MANUAL)
                if is_manual_alert_symbol(symbol):
                    ohlcv_m15 = ex_instance.fetch_ohlcv(symbol, timeframe='15m', limit=50)
                    ohlcv_h1 = ex_instance.fetch_ohlcv(symbol, timeframe='1h', limit=60)
                    ohlcv_d1 = ex_instance.fetch_ohlcv(symbol, timeframe='1d', limit=2)

                    if not ohlcv_m15 or not ohlcv_h1 or not ohlcv_d1:
                        continue

                    df_m15 = pd.DataFrame(ohlcv_m15, columns=['time', 'open', 'high', 'low', 'close', 'vol'])
                    df_h1 = pd.DataFrame(ohlcv_h1, columns=['time', 'open', 'high', 'low', 'close', 'vol'])
                    daily_open = ohlcv_d1[-1][1]

                    signal, entry, sl, tp = analyze_smc_retrace(df_m15, df_h1, daily_open)
                    if signal:
                        send_telegram(
                            f"📢 *[FX/COMMODITY ALERT - MANUAL ENTRY]*\n\n"
                            f"Pair: `{symbol}`\n"
                            f"Side: `{signal}`\n"
                            f"Entry Limit (50%): `{entry:.5f}`\n"
                            f"Stop Loss: `{sl:.5f}`\n"
                            f"Take Profit (1R): `{tp:.5f}`\n"
                            f"📌 _Eksekusi manual di broker FX / MT5 kamu!_"
                        )
                    continue

                # 2. CRYPTO PAIRS (AUTO-EXECUTE BINGX / MULTI-CEX)
                if active_crypto_count >= MAX_CONCURRENT_POSITIONS:
                    continue  # Hard Cap max 2 posisi crypto aktif

                ohlcv_m15 = ex_instance.fetch_ohlcv(symbol, timeframe='15m', limit=50)
                ohlcv_h1 = ex_instance.fetch_ohlcv(symbol, timeframe='1h', limit=60)
                ohlcv_d1 = ex_instance.fetch_ohlcv(symbol, timeframe='1d', limit=2)

                if not ohlcv_m15 or not ohlcv_h1 or not ohlcv_d1:
                    continue

                df_m15 = pd.DataFrame(ohlcv_m15, columns=['time', 'open', 'high', 'low', 'close', 'vol'])
                df_h1 = pd.DataFrame(ohlcv_h1, columns=['time', 'open', 'high', 'low', 'close', 'vol'])
                daily_open = ohlcv_d1[-1][1]

                signal, entry, sl, tp = analyze_smc_retrace(df_m15, df_h1, daily_open)

                if signal:
                    try:
                        ex_instance.set_leverage(LEVERAGE, symbol)
                    except Exception:
                        pass

                    qty = (MARGIN_PER_TRADE * LEVERAGE) / entry
                    order_side = 'buy' if signal == 'BUY' else 'sell'

                    order = ex_instance.create_order(
                        symbol=symbol,
                        type='limit',
                        side=order_side,
                        amount=qty,
                        price=entry,
                        params={'stopLoss': {'price': sl}, 'takeProfit': {'price': tp}}
                    )

                    conn = sqlite3.connect(DB_FILE)
                    c = conn.cursor()
                    c.execute("""
                        INSERT INTO trades (exchange_id, symbol, order_type, side, entry_price, sl_price, tp_price, order_id, status)
                        VALUES (?, ?, 'LIMIT', ?, ?, ?, ?, ?, 'PENDING')
                    """, (ex_id, symbol, signal, entry, sl, tp, str(order['id'])))
                    conn.commit()
                    conn.close()

                    send_telegram(
                        f"🎯 *[NEW CRYPTO SMC SIGNAL]*\n\n"
                        f"Exchange: `{ex_id.upper()}`\n"
                        f"Pair: `{symbol}`\n"
                        f"Side: `{signal}`\n"
                        f"Entry Limit (50%): `{entry:.5f}`\n"
                        f"Stop Loss: `{sl:.5f}`\n"
                        f"Take Profit (1R): `{tp:.5f}`\n"
                        f"Margin: `${MARGIN_PER_TRADE} USDT` ({LEVERAGE}x)"
                    )
                    active_crypto_count += 1

        except Exception as e:
            send_error_alert(f"Market Scan Exception [{ex_id}]: {str(e)}")

# =================================================================
# TELEGRAM COMMAND HANDLER (FILTERED & SECURE)
# =================================================================
def telegram_polling():
    last_update_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=10"
            res = requests.get(url, timeout=15).json()
            if res.get("ok"):
                for result in res.get("result", []):
                    last_update_id = result["update_id"]
                    message = result.get("message", {})
                    chat_id = str(message.get("chat", {}).get("id", ""))
                    
                    # FILTER KEAMANAN: Abaikan pesan dari ID asing!
                    if str(chat_id) != str(TELEGRAM_CHAT_ID):
                        continue

                    text = message.get("text", "").strip()

                    if text in ["/start", "/help"]:
                        msg = (
                            "🤖 *[ALCHEMIST PRO v2 COMMANDS]*\n\n"
                            "• `/posisi` - Cek status bot, saldo & posisi aktif\n"
                            "• `/pause` - Pause scanner bot secara manual\n"
                            "• `/resume` - Resume scanner bot secara manual\n"
                            "• `/cancelall` - Batalin semua pending limit order\n"
                            "• `/stat` - Ringkasan statistik trading\n"
                            "• `/help` - Tampilkan menu bantuan ini"
                        )
                        send_telegram(msg)

                    elif text == "/posisi":
                        conn = sqlite3.connect(DB_FILE)
                        c = conn.cursor()
                        c.execute("SELECT exchange_id, symbol, side, status FROM trades WHERE status IN ('PENDING', 'OPEN')")
                        active_trades = c.fetchall()
                        conn.close()

                        # Saldo per Exchange
                        bal_info = ""
                        for ex_k, ex_v in exchanges.items():
                            bal = get_free_usdt_balance(ex_v)
                            bal_info += f"• `{ex_k.upper()}` Free: `${bal:.2f} USDT`\n"

                        status_str = "PAUSED 🛑" if IS_BOT_PAUSED else "RUNNING 🟢"
                        msg = (
                            f"📊 *[STATUS BOT & POSISI]*\n\n"
                            f"Status Bot: `{status_str}`\n"
                            f"SL Hari Ini: `{daily_sl_count}/{MAX_DAILY_SL}`\n"
                            f"Max Posisi Allowed: `{MAX_CONCURRENT_POSITIONS}`\n\n"
                            f"*Saldo Bursa:* \n{bal_info}\n"
                            f"*Daftar Antrean/Posisi:* {len(active_trades)}\n"
                        )
                        for t in active_trades:
                            msg += f"• `[{t[0].upper()}]` `{t[1]}` | `{t[2]}` | `{t[3]}`\n"
                        send_telegram(msg)

                    elif text == "/pause":
                        global IS_BOT_PAUSED
                        IS_BOT_PAUSED = True
                        send_telegram("⏸️ Bot di-PAUSE secara manual via Telegram.")

                    elif text == "/resume":
                        IS_BOT_PAUSED = False
                        send_telegram("▶️ Bot di-RESUME dan kembali aktif scanning.")

                    elif text == "/cancelall":
                        cancel_all_pending_orders_on_pause()
                        send_telegram("🚨 *[EMERGENCY CANCEL]* Semua pending limit order berhasil dibatalkan!")

                    elif text == "/stat":
                        conn = sqlite3.connect(DB_FILE)
                        c = conn.cursor()
                        c.execute("SELECT status, COUNT(*) FROM trades GROUP BY status")
                        stats = c.fetchall()
                        conn.close()

                        msg = "📈 *[STATISTIK TRADING]*\n\n"
                        for s in stats:
                            msg += f"• `{s[0]}`: {s[1]}\n"
                        send_telegram(msg)

        except Exception:
            time.sleep(5)
        time.sleep(1)

# =================================================================
# FLASK KEEP-ALIVE SERVER
# =================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "Alchemist Pro HYBRID v2 Engine Running 24/7!"

def run_flask():
    app.run(host='0.0.0.0', port=5000)

# =================================================================
# MAIN SCHEDULER & EXECUTION LOOP
# =================================================================
def main_loop():
    last_heartbeat = time.time()
    send_telegram("🚀 *[ALCHEMIST PRO HYBRID v2 INITIALIZED]*\n\nBot berhasil di-running dan siap nyari sinyal!")

    while True:
        try:
            now_wib = datetime.now(WIB)

            # Reset Harian Jam 07:00 WIB
            if now_wib.hour == 7 and now_wib.minute == 0 and now_wib.second < 15:
                global daily_sl_count, IS_BOT_PAUSED
                daily_sl_count = 0
                if IS_BOT_PAUSED:
                    IS_BOT_PAUSED = False
                    send_telegram("🌅 *[NEW DAY RESET]*\n\nHitungan SL harian di-reset ke 0. Bot kembali aktif nyari sinyal!")

            # Heartbeat Ping tiap 6 Jam (21600 detik)
            if time.time() - last_heartbeat >= 21600:
                send_telegram("💓 *[HEARTBEAT PING]*\n\nSistem Alchemist Pro v2 beroperasi normal 24/7.")
                last_heartbeat = time.time()

            # Dynamic Loss & Order/Position Lifecycle Check
            check_and_update_daily_sl()
            update_orders_and_positions()

            # Market Scanner Execution
            if not IS_BOT_PAUSED and is_kill_zone():
                scan_markets()

        except Exception as e:
            send_error_alert(f"Main Loop Exception: {str(e)}")

        time.sleep(15)

# =================================================================
# ENTRY POINT
# =================================================================
if __name__ == '__main__':
    init_db()

    t_tele = threading.Thread(target=telegram_polling, daemon=True)
    t_tele.start()

    t_flask = threading.Thread(target=run_flask, daemon=True)
    t_flask.start()

    main_loop()
