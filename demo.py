import sys
import time
import json
import math
from kafka import KafkaProducer, KafkaConsumer
import logging
logging.basicConfig(level=logging.ERROR)


hosts = ['localhost:9092']
topic = 'kafka-demo'
topic_count = 'kafka-demo-count'
ident = "epic-iot-1"

def kafka_serializer(data):
	return json.dumps(data).encode('utf-8')

def kafka_deserializer(data):
    return json.loads(data.decode('ascii'))

demo_producer = KafkaProducer(bootstrap_servers=hosts, value_serializer=kafka_serializer, request_timeout_ms=1500, max_block_ms=1500)
demo_consumer = KafkaConsumer(topic, bootstrap_servers=hosts, value_deserializer=kafka_deserializer, request_timeout_ms=1500, consumer_timeout_ms=1500)
demo_count_consumer = KafkaConsumer(topic_count, bootstrap_servers=hosts, value_deserializer=kafka_deserializer, request_timeout_ms=1500, consumer_timeout_ms=1500)

while True:
    try:
        timestamp_ms = int(round(time.time() * 1000))
        payload = {
            "id": ident,
            "ts": timestamp_ms,
            "value": 1000 * math.sin(((timestamp_ms / 1000) % 360) / 360.0 * 2 * math.pi)
            }
        print(json.dumps(payload))
        demo_producer.send(topic, value=payload, timestamp_ms=timestamp_ms)
        time.sleep(1)

        # consumer all messages every 5 seconds to demonstrate we can read our own messages
        if int(timestamp_ms / 1000.0) % 10 == 0:
            for message in demo_consumer:
                print("Consumer: {ts} {id} {val}".format(ts=message.timestamp, id=message.value['id'], val=message.value['value']))
            for message in demo_count_consumer:
                print("Consumer Count: {ts} {id} {val}".format(ts=message.timestamp, id=message.value['id'], val=message.value['value']))

    except KeyboardInterrupt:
        print("Terminating gracefully...")
        sys.exit()
