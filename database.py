import sqlite3
import os
from datetime import datetime, timedelta

DB_NAME = 'bot_database.db'

def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        is_admin BOOLEAN DEFAULT FALSE,
        end_date DATETIME,
        is_paused BOOLEAN DEFAULT FALSE,
        pause_until DATETIME
    )
    ''')
    
    # Settings table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    ''')
    
    # Sent predictions table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sent_predictions (
        prediction_key TEXT PRIMARY KEY,
        sent_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # User predictions table (for signal limits)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        prediction_key TEXT,
        sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')
    
    # Bookmakers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bookmakers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        is_active BOOLEAN DEFAULT TRUE
    )
    ''')
    
    # User bookmakers table (many-to-many relationship)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_bookmakers (
        user_id INTEGER,
        bookmaker_id INTEGER,
        PRIMARY KEY (user_id, bookmaker_id),
        FOREIGN KEY (user_id) REFERENCES users (user_id),
        FOREIGN KEY (bookmaker_id) REFERENCES bookmakers (id)
    )
    ''')
    
    # Channels table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channels (
        channel_id INTEGER PRIMARY KEY,
        name TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Channel bookmakers table (many-to-many relationship)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channel_bookmakers (
        channel_id INTEGER,
        bookmaker_id INTEGER,
        is_selected BOOLEAN DEFAULT TRUE,
        PRIMARY KEY (channel_id, bookmaker_id),
        FOREIGN KEY (channel_id) REFERENCES channels (channel_id),
        FOREIGN KEY (bookmaker_id) REFERENCES bookmakers (id)
    )
    ''')
    
    # Signal limits table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS signal_limits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        max_signals_per_day INTEGER DEFAULT 10,
        pause_after_signals INTEGER DEFAULT 5,
        pause_duration_hours INTEGER DEFAULT 24
    )
    ''')
    
    # Insert default signal limits if not exists
    cursor.execute('SELECT COUNT(*) FROM signal_limits')
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
        INSERT INTO signal_limits (max_signals_per_day, pause_after_signals, pause_duration_hours)
        VALUES (10, 5, 24)
        ''')
    
    # Insert default bookmakers if not exists
    cursor.execute('SELECT COUNT(*) FROM bookmakers')
    if cursor.fetchone()[0] == 0:
        default_bookmakers = [
            'Betboom',
            'Fonbet', 'Marathon', 'Olimp', 'Winline'
        ]
        for bookmaker in default_bookmakers:
            cursor.execute('INSERT OR IGNORE INTO bookmakers (name) VALUES (?)', (bookmaker,))
    
    conn.commit()
    conn.close()

# User management functions
def add_user(user_id, username, is_admin=False):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR IGNORE INTO users (user_id, username, is_admin) VALUES (?, ?, ?)',
        (user_id, username, is_admin)
    )
    conn.commit()
    conn.close()

def get_user(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    conn.close()
    return dict(user) if user else None

def get_all_users():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users')
    users = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return users

def get_all_active_users():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE end_date > datetime("now") AND is_paused = FALSE')
    users = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return users

def update_subscription(user_id, days):
    conn = get_connection()
    cursor = conn.cursor()
    end_date = datetime.now() + timedelta(days=days)
    cursor.execute(
        'UPDATE users SET end_date = ? WHERE user_id = ?',
        (end_date.isoformat(), user_id)
    )
    conn.commit()
    conn.close()

def pause_subscription(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE users SET is_paused = TRUE WHERE user_id = ?',
        (user_id,)
    )
    conn.commit()
    conn.close()

def unpause_subscription(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE users SET is_paused = FALSE WHERE user_id = ?',
        (user_id,)
    )
    conn.commit()
    conn.close()

def cancel_subscription(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE users SET end_date = NULL WHERE user_id = ?',
        (user_id,)
    )
    conn.commit()
    conn.close()

def make_admin(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE users SET is_admin = TRUE WHERE user_id = ?',
        (user_id,)
    )
    conn.commit()
    conn.close()

# Settings functions
def set_setting(key, value):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)',
        (key, value)
    )
    conn.commit()
    conn.close()

def get_setting(key, default=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else default

# Prediction tracking functions
def add_sent_prediction(prediction_key):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR IGNORE INTO sent_predictions (prediction_key) VALUES (?)',
        (prediction_key,)
    )
    conn.commit()
    conn.close()

def is_prediction_sent(prediction_key):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT COUNT(*) FROM sent_predictions WHERE prediction_key = ?',
        (prediction_key,)
    )
    result = cursor.fetchone()[0] > 0
    conn.close()
    return result

def delete_old_predictions():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'DELETE FROM sent_predictions WHERE sent_at < datetime("now", "-7 days")'
    )
    conn.commit()
    conn.close()

# User prediction tracking for signal limits
def add_user_prediction(user_id, prediction_key):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO user_predictions (user_id, prediction_key) VALUES (?, ?)',
        (user_id, prediction_key)
    )
    conn.commit()
    conn.close()

def get_user_daily_signal_count(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        '''SELECT COUNT(*) FROM user_predictions 
           WHERE user_id = ? AND date(sent_at) = date('now')''',
        (user_id,)
    )
    count = cursor.fetchone()[0]
    conn.close()
    return count

def delete_old_user_predictions():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'DELETE FROM user_predictions WHERE sent_at < datetime("now", "-7 days")'
    )
    conn.commit()
    conn.close()

def get_recent_user_predictions(limit=10):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT * FROM user_predictions ORDER BY sent_at DESC LIMIT ?',
        (limit,)
    )
    predictions = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return predictions

def get_total_predictions_count():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM user_predictions')
    count = cursor.fetchone()[0]
    conn.close()
    return count

# User pause functionality
def set_user_pause(user_id, hours):
    conn = get_connection()
    cursor = conn.cursor()
    pause_until = datetime.now() + timedelta(hours=hours)
    cursor.execute(
        'UPDATE users SET is_paused = TRUE, pause_until = ? WHERE user_id = ?',
        (pause_until.isoformat(), user_id)
    )
    conn.commit()
    conn.close()

def is_user_paused(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT is_paused, pause_until FROM users WHERE user_id = ?',
        (user_id,)
    )
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return False
        
    is_paused, pause_until = result
    if not is_paused:
        return False
        
    if pause_until and datetime.fromisoformat(pause_until) < datetime.now():
        # Pause period has ended, resume user
        unpause_subscription(user_id)
        return False
        
    return True

def check_and_resume_users():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE users SET is_paused = FALSE WHERE pause_until < datetime("now")'
    )
    conn.commit()
    conn.close()

# Bookmaker management functions
def add_bookmaker(name):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR IGNORE INTO bookmakers (name) VALUES (?)',
        (name,)
    )
    conn.commit()
    conn.close()

def get_all_bookmakers():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM bookmakers ORDER BY name')
    bookmakers = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return bookmakers

def toggle_bookmaker(bookmaker_id, is_active):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE bookmakers SET is_active = ? WHERE id = ?',
        (is_active, bookmaker_id)
    )
    conn.commit()
    conn.close()

# User bookmaker preferences
def get_user_bookmakers(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT b.* FROM bookmakers b
        JOIN user_bookmakers ub ON b.id = ub.bookmaker_id
        WHERE ub.user_id = ? AND b.is_active = TRUE
    ''', (user_id,))
    bookmakers = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return bookmakers

def update_user_bookmakers(user_id, bookmaker_ids):
    conn = get_connection()
    cursor = conn.cursor()
    
    # Remove existing preferences
    cursor.execute('DELETE FROM user_bookmakers WHERE user_id = ?', (user_id,))
    
    # Add new preferences
    for bookmaker_id in bookmaker_ids:
        cursor.execute(
            'INSERT INTO user_bookmakers (user_id, bookmaker_id) VALUES (?, ?)',
            (user_id, bookmaker_id)
        )
    
    conn.commit()
    conn.close()

# Channel management functions
def add_channel(channel_id, name):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO channels (channel_id, name) VALUES (?, ?)',
        (channel_id, name)
    )
    conn.commit()
    
    # Initialize bookmaker settings for this channel
    bookmakers = get_all_bookmakers()
    for bookmaker in bookmakers:
        cursor.execute(
            'INSERT OR IGNORE INTO channel_bookmakers (channel_id, bookmaker_id, is_selected) VALUES (?, ?, ?)',
            (channel_id, bookmaker['id'], True)
        )
    
    conn.commit()
    conn.close()

def get_channel(channel_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM channels WHERE channel_id = ?', (channel_id,))
    channel = cursor.fetchone()
    conn.close()
    return dict(channel) if channel else None

def get_all_channels():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM channels ORDER BY name')
    channels = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return channels

def update_channel(channel_id, is_active=None, name=None):
    conn = get_connection()
    cursor = conn.cursor()
    
    if is_active is not None and name is not None:
        cursor.execute(
            'UPDATE channels SET is_active = ?, name = ? WHERE channel_id = ?',
            (is_active, name, channel_id)
        )
    elif is_active is not None:
        cursor.execute(
            'UPDATE channels SET is_active = ? WHERE channel_id = ?',
            (is_active, channel_id)
        )
    elif name is not None:
        cursor.execute(
            'UPDATE channels SET name = ? WHERE channel_id = ?',
            (name, channel_id)
        )
    
    conn.commit()
    conn.close()

def delete_channel(channel_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
    cursor.execute('DELETE FROM channel_bookmakers WHERE channel_id = ?', (channel_id,))
    conn.commit()
    conn.close()

# Channel bookmaker preferences
def get_channel_bookmakers(channel_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT b.*, cb.is_selected FROM bookmakers b
        JOIN channel_bookmakers cb ON b.id = cb.bookmaker_id
        WHERE cb.channel_id = ? AND b.is_active = TRUE
        ORDER BY b.name
    ''', (channel_id,))
    bookmakers = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return bookmakers

def update_channel_bookmaker(channel_id, bookmaker_id, is_selected):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO channel_bookmakers (channel_id, bookmaker_id, is_selected)
        VALUES (?, ?, ?)
    ''', (channel_id, bookmaker_id, is_selected))
    conn.commit()
    conn.close()

def get_selected_channel_bookmakers(channel_id=None):
    conn = get_connection()
    cursor = conn.cursor()
    
    if channel_id:
        cursor.execute('''
            SELECT b.* FROM bookmakers b
            JOIN channel_bookmakers cb ON b.id = cb.bookmaker_id
            WHERE cb.channel_id = ? AND cb.is_selected = TRUE AND b.is_active = TRUE
        ''', (channel_id,))
    else:
        cursor.execute('''
            SELECT b.* FROM bookmakers b
            JOIN channel_bookmakers cb ON b.id = cb.bookmaker_id
            WHERE cb.is_selected = TRUE AND b.is_active = TRUE
        ''')
    
    bookmakers = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return bookmakers

# Signal limits management
def get_signal_limits():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM signal_limits ORDER BY id DESC LIMIT 1')
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return dict(result)
    else:
        return {
            'max_signals_per_day': 10,
            'pause_after_signals': 5,
            'pause_duration_hours': 24
        }

def set_signal_limits(max_signals, pause_after, pause_hours):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO signal_limits (max_signals_per_day, pause_after_signals, pause_duration_hours)
        VALUES (?, ?, ?)
    ''', (max_signals, pause_after, pause_hours))
    conn.commit()
    conn.close()