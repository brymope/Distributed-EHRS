import sqlite3
import hashlib
import jwt
import datetime
import logging
from flask import Flask, request, jsonify
import config

app = Flask(__name__)
DB_NAME = "./db/iam.db"
JWT_SECRET = config.JWT_SECRET
JWT_EXPIRATION_HOURS = 24

#Console logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

#Database initialization. It creates a users table if it doesn't exist and adds a default admin user with username "admin" and password "admin". 
# The passwords are stored as SHA-256 hashes for basic security. 

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT
        )
    ''')
    # Create a default admin user (password: admin) for testing
    default_user = 'admin'
    default_pass = 'admin'
    password_hash = hashlib.sha256(default_pass.encode()).hexdigest()
    try:
        conn.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)',
                     (default_user, password_hash))
        log.info("Default admin user created (username=admin, password=admin)")
    except sqlite3.IntegrityError:
        pass  # user already exists
    conn.commit()
    conn.close()
    log.info("IAM database initialized")

init_db()

#helper function to verify username and password. 
def verify_password(username, password):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT password_hash FROM users WHERE username = ?', (username,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return False
    stored_hash = row[0]
    input_hash = hashlib.sha256(password.encode()).hexdigest()
    return stored_hash == input_hash

# Endpoint for user login.
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400

    if not verify_password(username, password):
        return jsonify({"error": "Invalid credentials"}), 401

    payload = {
        'username': username,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=JWT_EXPIRATION_HOURS)
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm='HS256')
    return jsonify({"token": token})

@app.route('/verify', methods=['POST'])
def verify():
    token = request.headers.get('Authorization')
    if not token or not token.startswith('Bearer '):
        return jsonify({"error": "Token required"}), 401
    token = token.split(' ')[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
    
        return jsonify({"valid": True, "username": payload.get('username')})
    except jwt.ExpiredSignatureError:
        return jsonify({"valid": False, "error": "Token expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"valid": False, "error": "Invalid token"}), 401

#Add another user endpoint for testing purposes. 
@app.route('/users', methods=['POST'])
def create_user():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400

    password_hash = hashlib.sha256(password.encode()).hexdigest()
    conn = sqlite3.connect(DB_NAME)
    try:
        conn.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)',
                     (username, password_hash))
        conn.commit()
        conn.close()
        return jsonify({"status": "User created"}), 201
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({"error": "Username already exists"}), 409

if __name__ == '__main__':
    log.info("Starting IAM service on port 7000")
    app.run(host='0.0.0.0', port=7000, debug=True)