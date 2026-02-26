import sys, json, sqlite3, logging, time, hashlib, jwt, threading, os, requests, pika, tempfile
from functools import wraps
from flask import Flask, request, jsonify, Response
from pysyncobj import SyncObj, SyncObjConf, replicated
import config

#console logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

#Database and Node configuration
node_id = config.NODE_ID
self_addr = config.CLUSTER_NODES[node_id]
http_port = int(self_addr.split(':')[1])
raft_port = http_port + 10
raft_self = f"{self_addr.split(':')[0]}:{raft_port}"

raft_partners = [
    f"{addr.split(':')[0]}:{int(addr.split(':')[1]) + 10}"
    for i, addr in config.CLUSTER_NODES.items() if i != node_id
]

if not os.path.exists("./db"): os.makedirs("./db")
DB_NAME = f"./db/patient_{node_id}.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("CREATE TABLE IF NOT EXISTS patients (healthcare_id TEXT PRIMARY KEY, patient_info TEXT)")
    conn.execute('''
        CREATE TABLE IF NOT EXISTS visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id TEXT,
            visit_info TEXT,
            FOREIGN KEY(patient_id) REFERENCES patients(healthcare_id)
        )
    ''')
    conn.commit()
    conn.close()
init_db()

#RAFT SETUP
#Raft-based Hospital Node Implementation using pysyncobj for state replication and consistency across the cluster. 
class HospitalNode(SyncObj):
    def __init__(self, selfAddress, partners, cfg):
        super().__init__(selfAddress, partners, cfg)
        self.data = {}
        self._load()

    def _load(self):
        conn = sqlite3.connect(DB_NAME)
        rows = conn.execute("SELECT healthcare_id, patient_info FROM patients").fetchall()
        for pid, pdata in rows:
            patient_dict = json.loads(pdata)
            v_rows = conn.execute("SELECT visit_info FROM visits WHERE patient_id=?", (pid,)).fetchall()
            patient_dict["visits"] = [json.loads(v[0]) for v in v_rows]
            self.data[pid] = patient_dict
        conn.close()

    @replicated
    def create_patient_log(self, pid, pdata, origin_node):
        if pid in self.data: return False
        self.data[pid] = pdata
        self._save_to_disk(pid, pdata)
        if self._isLeader():
            publish_event("CREATE", pid, pdata, origin_node)
        return True

    @replicated
    def update_patient_log(self, pid, update_data, origin_node, is_visit=False):
        if pid not in self.data: return False
        if is_visit:
            self._save_visit_to_disk(pid, update_data)
            self.data[pid]["visits"].append(update_data)
        else:
            for k, v in update_data.items():
                if k != "visits": self.data[pid][k] = v
            self._save_to_disk(pid, self.data[pid])
        
        if self._isLeader():
            event_type = "VISIT" if is_visit else "UPDATE"
            publish_event(event_type, pid, update_data, origin_node)
        return True

    @replicated
    def delete_patient_log(self, pid, origin_node):
        if pid not in self.data: return False
        self.data.pop(pid, None)
        conn = sqlite3.connect(DB_NAME)
        conn.execute("DELETE FROM patients WHERE healthcare_id=?", (pid,))
        conn.execute("DELETE FROM visits WHERE patient_id=?", (pid,))
        conn.commit()
        conn.close()
        if self._isLeader():
            publish_event("DELETE", pid, {}, origin_node)
        return True

    def _save_to_disk(self, pid, pdata):
        conn = sqlite3.connect(DB_NAME)
        clean_data = {k:v for k,v in pdata.items() if k != "visits"}
        conn.execute("INSERT OR REPLACE INTO patients VALUES (?,?)", (pid, json.dumps(clean_data)))
        conn.commit()
        conn.close()

    def _save_visit_to_disk(self, pid, visit_data):
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT INTO visits (patient_id, visit_info) VALUES (?,?)", (pid, json.dumps(visit_data)))
        conn.commit()
        conn.close()

    def get_patient(self, pid): return self.data.get(pid)
    def get_all(self): return dict(self.data)

#Flask App Setup including authentication and route definitions for patient record management.
app = Flask(__name__)
raft_node = HospitalNode(raft_self, raft_partners, SyncObjConf(
    autoTick=True, appendEntriesUseBatch=True,
    journalFile=f"{tempfile.gettempdir()}/jrnl_{node_id}.dat",
    fullDumpFile=f"{tempfile.gettempdir()}/dmp_{node_id}.dat"
))

#method for when authentication is required for certain routes, checks for a valid JWT token in the Authorization header and decodes it to get the username. If the token is missing or invalid, it returns a 401 error response.
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization")
        if not auth or not auth.startswith("Bearer "): return jsonify({"error": "Token required"}), 401
        try: 
            payload = jwt.decode(auth.split()[1], config.JWT_SECRET, algorithms=["HS256"])
            request.user = payload.get('username')
        except: return jsonify({"error": "Invalid token"}), 401
        return f(*args, **kwargs)
    return decorated

# RabbitMQ event publishing function that sends patient-related events to the configured exchange with appropriate routing keys. 
def publish_event(event_type, pid, details, origin_node, status="SUCCESS"):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.RABBITMQ_HOST))
        channel = connection.channel()
        channel.exchange_declare(exchange=config.RABBITMQ_EXCHANGE, exchange_type='topic', durable=True)
        
        for node in config.CLUSTER_NODES.keys():
            aq = f"audit_queue_{node}"
            mq = f"ml_worker_{node}_queue"
            channel.queue_declare(queue=aq, durable=True)
            channel.queue_declare(queue=mq, durable=True)
            channel.queue_bind(exchange=config.RABBITMQ_EXCHANGE, queue=aq, routing_key="audit.patient.#")
            channel.queue_bind(exchange=config.RABBITMQ_EXCHANGE, queue=mq, routing_key="audit.patient.#")

        payload = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source_node": origin_node, 
            "type": event_type,
            "status": status,
            "patient_id": pid,
            "details": details 
        }
        
        routing_key = f"audit.patient.{event_type.lower()}.{status.lower()}"
        channel.basic_publish(
            exchange=config.RABBITMQ_EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
    except Exception as e:
        log.error(f"RabbitMQ Publish Failed: {e}")

# Helper function to route requests to the current Raft leader if the node receiving the request is not the leader. 
def handle_leader_routing(path):
    leader = raft_node._getLeader()
    if not leader: return jsonify({"error": "No Leader"}), 503
    leader_http = f"{leader.host}:{leader.port - 10}"
    if leader_http == self_addr: return None
    
    headers = {k:v for k,v in request.headers.items() if k.lower() != 'host'}
    headers['X-Origin-Node'] = config.NODE_ID
    
    try:
        resp = requests.request(method=request.method, url=f"http://{leader_http}{path}",
                                headers=headers,
                                data=request.get_data(), timeout=5)
        return Response(resp.content, resp.status_code, resp.headers.items())
    except: return jsonify({"error": "Leader Unreachable"}), 502

def get_origin():
    return request.headers.get('X-Origin-Node', config.NODE_ID)


# Health check endpoint to verify the status of the node and its role in the Raft cluster. 
@app.route('/status')
def status():
    l = raft_node._getLeader()
    return jsonify({"node": node_id, "role": "LEADER" if (l and f"{l.host}:{l.port}" == raft_self) else "FOLLOWER", "connected": len(raft_node.otherNodes)})


# Endpoint to retrieve all patient records from the local Raft state. 
@app.route('/patients', methods=['GET'])
@token_required
def get_all_patients():
    """Returns all patients from the local Raft state."""
    all_data = raft_node.get_all()
    
    patient_list = [{"id": k, **v} for k, v in all_data.items()]
    
    return jsonify({
        "total": len(patient_list),
        "source_node": node_id,
        "patients": patient_list
    }), 200

@app.route('/patient', methods=['POST'])
@token_required
def create_p():
    proxy = handle_leader_routing("/patient")
    if proxy: return proxy
    data = request.json
    pid = hashlib.sha256(f"{data['ssn']}|{config.HASH_SALT}".encode()).hexdigest()[:6]
    record = {"name": data["name"], "dob": data["dob"], "gender": data["gender"], "visits": []}
    
    origin = get_origin()
    if raft_node.create_patient_log(pid, record, origin_node=origin, sync=True):
        return jsonify({"created": pid}), 201
    return jsonify({"error": "Exists"}), 409

@app.route('/patient/<pid>', methods=['PUT'])
@token_required
def update_p(pid):
    proxy = handle_leader_routing(f"/patient/{pid}")
    if proxy: return proxy
    origin = get_origin()
    if raft_node.update_patient_log(pid, request.json, origin_node=origin, sync=True):
        return jsonify({"status": "Updated"}), 200
    return jsonify({"error": "Not found"}), 404

@app.route('/patient/<pid>/visit', methods=['POST'])
@token_required
def add_v(pid):
    # Check if the processing node is the leader, if not forward the request to the leader to maintain consistency. 
   
    leader = raft_node._getLeader()
    if leader and f"{leader.host}:{leader.port - 10}" != self_addr:
        leader_http = f"{leader.host}:{leader.port - 10}"
        try:
             headers = {k:v for k,v in request.headers.items() if k.lower() != 'host'}
             headers['X-Origin-Node'] = config.NODE_ID
             resp = requests.post(f"http://{leader_http}/patient/{pid}/visit",
                                  headers=headers, json=request.json, timeout=5)
             return Response(resp.content, resp.status_code, resp.headers.items())
        except: return jsonify({"error": "Leader Unreachable"}), 502

    origin = get_origin()
    visit_data = request.json
    visit_data['hospital'] = origin 
    
    if raft_node.update_patient_log(pid, visit_data, origin_node=origin, is_visit=True, sync=True):
        return jsonify({"status": "Visit added"}), 200
    return jsonify({"error": "Not found"}), 404

@app.route('/patient/<pid>', methods=['DELETE'])
@token_required
def delete_p(pid):
    proxy = handle_leader_routing(f"/patient/{pid}")
    if proxy: return proxy
    origin = get_origin()
    if raft_node.delete_patient_log(pid, origin_node=origin, sync=True):
        return jsonify({"status": "Deleted"}), 200
    return jsonify({"error": "Not found"}), 404

@app.route('/patient/<pid>', methods=['GET'])
@token_required
def get_p(pid):
    p = raft_node.get_patient(pid)
    origin = get_origin() 
    
    if p:
        if request.user != 'system_ml_worker':
            publish_event("READ", pid, {"info": "Patient record accessed"}, origin_node=origin)
        return jsonify(p)
    
    if request.user != 'system_ml_worker':
        publish_event("READ", pid, {"info": "Patient not found"}, origin_node=origin, status="FAILURE")
        
    return jsonify({"error": "Not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=http_port, threaded=True)