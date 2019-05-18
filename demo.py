import sys
import time
import json
import math
from kafka import KafkaProducer


hosts = ['localhost:9092']

def kafka_serializer(data):
	return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=hosts, value_serializer=kafka_serializer)

topic = 'kafka-demo'
ident = "epic-iot-1"

while !end:
	try:
		timestamp_ms = int(round(time.time() * 1000))
		payload = {
			"id": ident,
			"value": math.sin(timestamp_ms)
		}
		print(json.dumps(payload))
		producer.send(topic, value=payload, timestamp_ms=timestamp_ms)
        time.sleep(1)
    except KeyboardInterrupt:
        print("Terminating gracefully...")
        sys.exit()
