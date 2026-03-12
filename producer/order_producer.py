import json
import random
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "orders"

def wait_for_kafka():
    import socket
    host, port = KAFKA_BOOTSTRAP_SERVERS.split(":")
    while True:
        try:
            s = socket.create_connection((host, int(port)), timeout=5)
            s.close()
            print("Kafka is ready")
            break
        except (socket.error, ConnectionRefusedError):
            print("Waiting for Kafka...")
            time.sleep(3)

wait_for_kafka()
time.sleep(5)  # extra buffer

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

PRODUCTS = ["Laptop", "Phone", "Tablet", "Headphones", "Watch", "Camera"]
CITIES   = ["Chennai", "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]

def generate_order(is_late=False):
    """
    is_late=True  → event_time is 2-4 hours in the past (late arriving)
    is_late=False → event_time is now (normal)
    """
    if is_late:
        delay_minutes = random.randint(2, 4)       # 2–4 minutes late (matches watermark)
    else:
        delay_minutes = random.randint(0, 1)        # almost real-time

    event_time   = datetime.utcnow() - timedelta(minutes=delay_minutes)
    ingest_time  = datetime.utcnow()

    return {
        "order_id"    : fake.uuid4(),
        "customer_id" : random.randint(1, 200),
        "product"     : random.choice(PRODUCTS),
        "amount"      : round(random.uniform(500, 50000), 2),
        "city"        : random.choice(CITIES),
        "status"      : random.choice(["placed", "confirmed", "shipped"]),
        "event_time"  : event_time.isoformat(),
        "ingest_time" : ingest_time.isoformat(),
        "is_late"     : is_late
    }

print(f"Producer started, sending to topic: {TOPIC}")

order_count = 0
while True:
    is_late = random.random() < 0.20
    order   = generate_order(is_late=is_late)

    if is_late:
        file_path = f"/app/late_data/late_order_{order['order_id'][:8]}.json"
        with open(file_path, "w") as f:
            json.dump(order, f)
        print(f"Late data saved: {file_path}")
    
    producer.send(TOPIC, order)
    order_count += 1

    tag = "LATE" if is_late else "NORMAL"
    print(f"[{order_count}] {tag} | id={order['order_id'][:8]} | "
          f"amt={order['amount']} | city={order['city']} | "
          f"ts={order['event_time']}")

    producer.flush()
    time.sleep(random.uniform(0.5, 2.0))   # random interval between messages
