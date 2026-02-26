import sqlite3
import pika
import logging
import json
import threading
import time
import os
import sys
import config
from flask import Flask, jsonify, request

# Console logging setup
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# Audit Database setup
DB_NAME = f"./db/audit_{config.NODE_ID}.db"

def init_db():
    if not os.path.exists("./db"):
        os.makedirs("./db")
    conn = sqlite3.connect(DB_NAME)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            source_node TEXT,
            event_type TEXT,
            patient_id TEXT,
            details TEXT,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()
    log.info(f"Audit Database initialized: {DB_NAME}")

#helper function to store events in the database
def store_event(event):
    """Writes the RabbitMQ message data into SQLite."""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute(
            "INSERT INTO audit_log (timestamp, source_node, event_type, patient_id, details, status) VALUES (?,?,?,?,?,?)",
            (
                event.get('timestamp'), 
                event.get('source_node'), 
                event.get('type'), 
                event.get('patient_id'), 
                json.dumps(event.get('details')),
                event.get('status', 'SUCCESS')
            )
        )
        conn.commit()
        conn.close()
        log.info(f"Audit Saved: {event.get('type')} for Patient {event.get('patient_id')} from Source {event.get('source_node')}")
    except Exception as e:
        log.error(f"Error writing to Audit DB: {e}")

#RabbitMQ Consumer setup to listen for audit events and store them in the database
def start_rabbitmq_consumer():
    """Background thread to listen for audit events."""
    while True:
        try:
            log.info("Connecting to RabbitMQ...")
            params = pika.ConnectionParameters(
                host=config.RABBITMQ_HOST,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.exchange_declare(
                exchange=config.RABBITMQ_EXCHANGE, 
                exchange_type='topic', 
                durable=config.RABBITMQ_DURABLE
            )

            # Unique queue name per hospital ensures EVERY hospital gets a copy
            queue_name = f"audit_queue_{config.NODE_ID}"
            channel.queue_declare(queue=queue_name, durable=True)

            # Bind to ALL patient events
            channel.queue_bind(
                exchange=config.RABBITMQ_EXCHANGE, 
                queue=queue_name, 
                routing_key="audit.patient.#"
            )

            def callback(ch, method, properties, body):
                try:
                    event_data = json.loads(body)
                    store_event(event_data)
                except Exception as e:
                    log.error(f"Failed to process message: {e}")

            channel.basic_consume(
                queue=queue_name, 
                on_message_callback=callback, 
                auto_ack=True
            )

            log.info(f"Audit Service active. Listening on: {queue_name}")
            channel.start_consuming()

        except Exception as e:
            log.warning(f"RabbitMQ connection lost ({e}). Retrying in 5 seconds...")
            time.sleep(5)

# Routes for API to retrieve audit logs

@app.route('/audit', methods=['GET'])
def get_audit_logs():
    """Returns the last 100 audit entries."""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 100")
        rows = cursor.fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/audit/patient/<pid>', methods=['GET'])
def get_patient_history(pid):
    """Returns all audit events for a specific patient ID."""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM audit_log WHERE patient_id = ? ORDER BY id DESC", (pid,))
        rows = cursor.fetchall()
        conn.close()
        return jsonify([dict(row) for row in rows]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    init_db()

    #RabbitMQ consumer runs in a separate thread to continuously listen for events
    consumer_thread = threading.Thread(target=start_rabbitmq_consumer, daemon=True)
    consumer_thread.start()

    #Calculate API port based on NODE_ID to avoid conflicts when running multiple hospitals on the same machine
    try:
        node_index = int(''.join(filter(str.isdigit, config.NODE_ID)))
    except:
        node_index = 1
        
    api_port = 6000 + node_index
    
    log.info(f"Audit API starting on port {api_port}...")
    app.run(host="0.0.0.0", port=api_port, debug=False, use_reloader=False)