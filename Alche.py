import os
import sys
import time
import sqlite3
import threading
import pandas as pd
import requests
import ccxt
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify

# =================================================================
# CONFIGURATION & PARAMETERS
# =================================================================
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

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
