NODE_ID = 'iam'

CLUSTER_NODES = {
    'hospital1': 'hospital1:5001',
    'hospital2': 'hospital2:5002',
    'hospital3': 'hospital3:5003'
}


RABBITMQ_HOST = "rabbitmq"
RABBITMQ_EXCHANGE = "audit_events"
RABBITMQ_DURABLE = True
RABBITMQ_PORT = 5672


HASH_SALT = "local-test-salt-12345"
JWT_SECRET = "df864b0775bad985334481f632f4cc2e"