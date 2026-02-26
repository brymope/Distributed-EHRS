import pika
import json
import config
import logging
import sqlite3
import threading
import time
import sys
import requests
import jwt
from datetime import datetime, timedelta
from flask import Flask, jsonify

#Console logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# Database setup
PRED_DB = f"./db/predictions_{config.NODE_ID}.db"
db_lock = threading.Lock()

def init_pred_db():
    if not sys.modules.get('os').path.exists("./db"):
        sys.modules.get('os').makedirs("./db")
    with db_lock:
        conn = sqlite3.connect(PRED_DB)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS patient_risk (
                healthcare_id TEXT PRIMARY KEY,
                risk_score REAL,
                prediction_time TEXT,
                rule_version TEXT
            )
        ''')
        conn.commit()
        conn.close()

init_pred_db()

#Computing risk score based on age, recent visits, and discharge recency. Using LACE-like logic. 
def calculate_age(dob_str):
    try:
        born = datetime.strptime(dob_str, '%Y-%m-%d')
        today = datetime.now()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    except Exception:
        return 40

def compute_risk_score(patient_record):
    if not patient_record: return 0.0
    
    if isinstance(patient_record, str):
        try:
            patient_record = json.loads(patient_record)
        except: return 0.0
        
    visits = patient_record.get('visits', [])
    if not visits: return 0.0
    
    today = datetime.now().date()
    age = calculate_age(patient_record.get('dob', '1980-01-01'))

    recent_visits = 0
    days_since_discharge = 999

    for v in visits:
        try:
    
            v_date_str = v.get('date') or v.get('visit_date')
            if v_date_str:
                visit_date = datetime.strptime(v_date_str, '%Y-%m-%d').date()
                if (today - visit_date).days <= 30:
                    recent_visits += 1
            
            if 'discharge_date' in v:
                d_date = datetime.strptime(v['discharge_date'], '%Y-%m-%d').date()
                diff = (today - d_date).days
                if diff >= 0 and diff < days_since_discharge: 
                    days_since_discharge = diff
        except Exception as e:
            log.debug(f"Date parse error in risk calc: {e}")

    age_factor = age / 100.0
    recent_visits_factor = recent_visits * 0.1
    discharge_recency_factor = max(0, 1.0 - (days_since_discharge / 30.0)) if days_since_discharge < 30 else 0.0

    # Risk Formula: (Age 40%) + (Visits 40%) + (Recency 20%)
    raw_risk = (age_factor * 0.4) + (recent_visits_factor * 0.4) + (discharge_recency_factor * 0.2)
    risk = min(1.0, max(0.0, raw_risk))
    return round(risk, 3)

def store_prediction(healthcare_id, risk, rule_version="v1"):
    with db_lock:
        conn = sqlite3.connect(PRED_DB)
        conn.execute('''
            INSERT OR REPLACE INTO patient_risk (healthcare_id, risk_score, prediction_time, rule_version)
            VALUES (?, ?, datetime('now'), ?)
        ''', (healthcare_id, risk, rule_version))
        conn.commit()
        conn.close()
    log.info(f"Updated risk score: {risk} for {healthcare_id}")

#helper function to fetch full patient record with retry mechanism to handle Raft replication lag. 
def fetch_full_patient_record(pid):
    patient_service_addr = config.CLUSTER_NODES[config.NODE_ID]
    
    payload = {
        'username': 'system_ml_worker',
        'exp': datetime.utcnow() + timedelta(minutes=5)
    }
    token = jwt.encode(payload, config.JWT_SECRET, algorithm='HS256')
    headers = {"Authorization": f"Bearer {token}"}
    
    for attempt in range(4): 
        try:
            resp = requests.get(f"http://{patient_service_addr}/patient/{pid}", headers=headers, timeout=2)
            if resp.status_code == 200:
                return resp.json()
            
            if resp.status_code == 404:
                log.warning(f"Attempt {attempt+1}: PID {pid} not found yet (replication lag). Retrying...")
            else:
                log.warning(f"Attempt {attempt+1}: Service returned {resp.status_code}")
                
        except Exception as e:
            log.error(f"Fetch Error: {e}")
        
        time.sleep(0.5) # Wait for Raft state machine to apply log
        
    return None

#RabbitMQ callback function to process incoming messages. 
def callback(ch, method, properties, body):
    try:
        msg = json.loads(body)
        op = msg.get('type', '').upper()
        hid = msg.get('patient_id')
        status = msg.get('status', 'SUCCESS')

        if status != "SUCCESS" or not hid or op == "READ":
            return

        if op == 'DELETE':
            with db_lock:
                conn = sqlite3.connect(PRED_DB)
                conn.execute('DELETE FROM patient_risk WHERE healthcare_id = ?', (hid,))
                conn.commit()
                conn.close()
            log.info(f"Deleted risk for {hid}")
        else:
           
            time.sleep(0.1)
            full_record = fetch_full_patient_record(hid)
            if full_record:
                risk = compute_risk_score(full_record)
                store_prediction(hid, risk)
            
    except Exception as e:
        log.error(f"ML Worker Error: {e}")

#Endpoint to retrieve the latest risk score for a patient. 
app_ml = Flask(__name__)

@app_ml.route('/risk/<pid>', methods=['GET'])
def get_risk(pid):
    try:
        with db_lock:
            conn = sqlite3.connect(PRED_DB)
            row = conn.execute("SELECT risk_score, prediction_time FROM patient_risk WHERE healthcare_id=?", (pid,)).fetchone()
            conn.close()
        
        if row:
            return jsonify({"patient_id": pid, "risk_score": row[0], "computed_at": row[1]})
        return jsonify({"error": "No risk score found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def run_api_server():
    try:
        node_index = int(''.join(filter(str.isdigit, config.NODE_ID)))
    except:
        node_index = 1
    
    port = 7000 + node_index
    log.info(f"ML API starting on port {port}...")
    app_ml.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# Main function to start the API server in a separate thread and then connect to RabbitMQ to consume messages.
def main():
    api_thread = threading.Thread(target=run_api_server, daemon=True)
    api_thread.start()

    log.info("ML Worker starting Consumer...")
    retries = 0
    connected = False

    while not connected and retries < 20:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=config.RABBITMQ_HOST)
            )
            channel = connection.channel()
            channel.exchange_declare(
                exchange=config.RABBITMQ_EXCHANGE,
                exchange_type='topic',
                durable=config.RABBITMQ_DURABLE
            )
            queue_name = f"ml_worker_{config.NODE_ID}_queue"
            channel.queue_declare(queue=queue_name, durable=True)
            channel.queue_bind(exchange=config.RABBITMQ_EXCHANGE, queue=queue_name, routing_key="audit.patient.#")
            
            log.info(f"Connected to RabbitMQ on queue {queue_name}")
            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
            connected = True
        except Exception as e:
            retries += 1
            log.warning(f"RabbitMQ connection issue: {e}. Retry {retries}/20 in 5s...")
            time.sleep(5)

if __name__ == '__main__':
    main()