import sys
import time
import json
import math
from kafka import KafkaProducer
import logging
logging.basicConfig(level=logging.ERROR)


hosts = ['localhost:9092']

def kafka_serializer(data):
	return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=hosts, value_serializer=kafka_serializer, request_timeout_ms=1500, max_block_ms=1500)

topic = 'kafka-demo'
ident = "epic-iot-1"

while True:
    try:
        timestamp_ms = int(round(time.time() * 1000))
        payload = {
            "id": ident,
            "value": 1000 * math.sin(((timestamp_ms / 1000) % 360) / 360.0 * 2 * math.pi)
            }
        print(json.dumps(payload))
        producer.send(topic, value=payload, timestamp_ms=timestamp_ms)
        time.sleep(1)
    except KeyboardInterrupt:
        print("Terminating gracefully...")
        sys.exit()
