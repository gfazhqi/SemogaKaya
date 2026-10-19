import os
import sqlite3
import requests
from flask import Flask, request, jsonify
import ccxt

# Modul MT5 untuk Forex (Opsional / Siap Pakai Nanti)
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False

app = Flask(__name__)

# =================================================================
# KONFIGURASI BOT & AKUN (ALCHEMIST SMC)
# =================================================================
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "ALCHEMIST_SMC_SECRET_123")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "MASUKKAN_TOKEN_BOT_TELEGRAM")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "MASUKKAN_CHAT_ID_TELEGRAM")

# Konfigurasi CEX (Default: Bitget. Bisa diganti 'bingx' atau 'binance')
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "bitget")
API_KEY = os.getenv("API_KEY", "MASUKKAN_API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY", "MASUKKAN_SECRET_KEY")
PASSPHRASE = os.getenv("PASSPHRASE", "MASUKKAN_PASSPHRASE_API")

# =================================================================
# INISIALISASI DATABASE SQLITE UNTUK STATISTIK
# =================================================================
DB_FILE = "trades.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT,
            entry_price REAL,
            exit_price REAL,
            margin REAL,
            leverage INTEGER,
            pnl REAL,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def log_open_trade(symbol, action, entry_price, margin, leverage):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO trades (symbol, action, entry_price, margin, leverage, status)
        VALUES (?, ?, ?, ?, ?, 'OPEN')
    ''', (symbol, action, entry_price, margin, leverage))
    conn.commit()
    conn.close()

def log_close_trade(symbol, exit_price, pnl, status_type):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        UPDATE trades
        SET exit_price = ?, pnl = ?, status = ?
        WHERE id = (
            SELECT id FROM trades
            WHERE symbol = ? AND status = 'OPEN'
            ORDER BY id DESC LIMIT 1
        )
    ''', (exit_price, pnl, status_type, symbol))
    conn.commit()
    conn.close()

def get_weekly_stats():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        SELECT status, pnl FROM trades
        WHERE status IN ('TP', 'SL', 'CLOSED')
          AND created_at >= datetime('now', '-7 days')
    ''')
    rows = c.fetchall()
    conn.close()

    if not rows:
        return None

    total_trades = len(rows)
    wins = sum(1 for r in rows if (r[1] is not None and r[1] > 0) or r[0] == 'TP')
    losses = sum(1 for r in rows if (r[1] is not None and r[1] < 0) or r[0] == 'SL')
    total_pnl = sum(r[1] for r in rows if r[1] is not None)
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0

    return {
        'total': total_trades,
        'wins': wins,
        'losses': losses,
        'win_rate': win_rate,
        'pnl': total_pnl
    }

# Setup CCXT Exchange Client
def get_exchange():
    exchange_class = getattr(ccxt, EXCHANGE_NAME)
    config = {
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    }
    if PASSPHRASE and EXCHANGE_NAME in ['bitget', 'kucoin']:
        config['password'] = PASSPHRASE
    return exchange_class(config)

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': msg,
        'parse_mode': 'Markdown'
    }
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"Gagal kirim notifikasi Telegram: {e}")

def set_safe_leverage(exchange, symbol, target_leverage=50):
    fallback_leverages = [50, 25, 20, 10, 5]
    active_lev = 1
    for lev in fallback_leverages:
        if lev > target_leverage:
            continue
        try:
            exchange.set_leverage(lev, symbol)
            active_lev = lev
            break
        except Exception:
            continue
    return active_lev

# =================================================================
# LOGIKA EKSEKUSI CRYPTO
# =================================================================
def execute_crypto(data):
    symbol = data.get('symbol')
    action = data.get('action').lower()  # "buy", "sell", "close", "tp", "sl"
    margin_usdt = float(data.get('margin_usdt', 2.0))
    target_leverage = int(data.get('leverage', 50))
    pnl = data.get('pnl', None)  # PnL opsional dari TV jika ada

    if "/" not in symbol and "USDT" in symbol:
        base = symbol.replace("USDT", "")
        symbol_formatted = f"{base}/USDT:USDT"
    else:
        symbol_formatted = symbol

    try:
        exchange = get_exchange()
        ticker = exchange.fetch_ticker(symbol_formatted)
        current_price = ticker['last']

        # LOGIKA TUTO/CLOSE (TP / SL)
        if action in ['close', 'tp', 'sl']:
            status_tag = action.upper()
            log_close_trade(symbol_formatted, current_price, pnl, status_tag)
            
            msg = (
                f"🏁 *[TRADE CLOSED - {status_tag}]*\n\n"
                f"• *Pair:* `{symbol_formatted}`\n"
                f"• *Harga Exit:* `${current_price}`\n"
                f"• *Est. PnL:* `{f'{pnl:+.2f} USDT' if pnl is not None else 'N/A'}`"
            )
            send_telegram(msg)
            return True, "Trade Closed"

        # LOGIKA ENTRY (BUY / SELL)
        active_lev = set_safe_leverage(exchange, symbol_formatted, target_leverage)
        position_size_usdt = margin_usdt * active_lev
        amount = position_size_usdt / current_price

        side = 'buy' if action == 'buy' else 'sell'
        order = exchange.create_order(symbol_formatted, 'market', side, amount)

        # Simpan ke Database local
        log_open_trade(symbol_formatted, action.upper(), current_price, margin_usdt, active_lev)

        msg = (
            f"🚀 *[AUTO-TRADE ENTRY]*\n\n"
            f"• *Exchange:* `{EXCHANGE_NAME.upper()}`\n"
            f"• *Pair:* `{symbol_formatted}`\n"
            f"• *Aksi:* `{action.upper()}`\n"
            f"• *Margin:* `${margin_usdt}` (Leverage `{active_lev}x`)\n"
            f"• *Total Posisi:* `${position_size_usdt:.2f}`\n"
            f"• *Harga Entry:* `${current_price}`"
        )
        send_telegram(msg)
        return True, order

    except Exception as e:
        err_msg = f"❌ *[AUTO-TRADE EROR]*\n\n• *Pair:* `{symbol}`\n• *Eror:* `{str(e)}`"
        send_telegram(err_msg)
        return False, str(e)

# =================================================================
# ENDPOINT TELEGRAM COMMAND LISTENER (/posisi & /stat)
# =================================================================
@app.route('/telegram_webhook', methods=['POST'])
def telegram_webhook():
    update = request.get_json()
    if not update or 'message' not in update:
        return 'ok', 200

    message = update['message']
    text = message.get('text', '').strip()
    chat_id = str(message['chat']['id'])

    if chat_id != TELEGRAM_CHAT_ID:
        return 'ok', 200

    # Command /posisi
    if text == '/posisi':
        try:
            exchange = get_exchange()
            positions = exchange.fetch_positions()
            open_pos = [p for p in positions if float(p.get('contracts', 0)) > 0]

            if not open_pos:
                send_telegram("📭 Nggak ada posisi yang lagi open saat ini.")
            else:
                msg = "📊 *POSISI RUNNING SAAT INI*\n\n"
                for p in open_pos:
                    side = p.get('side', '').upper()
                    pnl = float(p.get('unrealizedPnl', 0.0))
                    msg += (
                        f"• `{p['symbol']}` ({side})\n"
                        f"  Entry: `${p.get('entryPrice')}` | PnL: `{pnl:+.2f} USDT`\n"
                    )
                send_telegram(msg)
        except Exception as e:
            send_telegram(f"❌ Gagal mengambil posisi: `{str(e)}`")

    # Command /stat /mingguan
    elif text in ['/stat', '/mingguan']:
        stats = get_weekly_stats()
        if not stats:
            send_telegram("📭 Belum ada riwayat trade yang selesai dalam 7 hari terakhir.")
        else:
            msg = (
                f"📈 *REKAP STATISTIK TRADING (7 HARI)*\n\n"
                f"• *Total Trade:* `{stats['total']}`\n"
                f"• *Win / Loss:* `{stats['wins']} W` / `{stats['losses']} L`\n"
                f"• *Win Rate:* `{stats['win_rate']:.1f}%`\n"
                f"• *Net PnL:* `{stats['pnl']:+.2f} USDT`"
            )
            send_telegram(msg)

    return 'ok', 200

# =================================================================
# ENDPOINT WEBHOOK PENERIMA ALERT TRADINGVIEW
# =================================================================
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()

    if not data or data.get('passphrase') != WEBHOOK_PASSPHRASE:
        return jsonify({'status': 'error', 'message': 'Akses Ditolak'}), 401

    market_type = data.get('market', 'crypto').lower()

    if market_type == 'crypto':
        success, res = execute_crypto(data)
        return jsonify({'status': 'success' if success else 'error', 'message': res}), 200 if success else 500
    else:
        return jsonify({'status': 'error', 'message': 'Market type tidak dikenal'}), 400

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
