# Kafka Demo

Demonstrates writing to and processing simple timeseries data.

Producer generates a double sinusoid.

Python Consumer just echoes whatever is on the queue.

Clojure consumer calculates a first-order derivative and latest state.
Note that Clojure consumer needs to be ran after python script.

## Setup (Kafka)

Starto docker!
```bash
docker-compose up -d
```

Add kafka-demo topic inside docker
```
docker exec -it kafkademo_kafka_1 /bin/bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic kafka-demo
```

## Setup (Python)

Install virtualenvwrapper, docker, docker-compose.

Create a virtual env:
```bash
mkvirtualenv --python=/usr/bin/python3 kafka-demo
```

Install requirements:
```bash
pip install --requirements requirements.txt
```

Run script and enjoy.
```bash
python demo.py
```

## Setup (Clojure)

Install leiningen.

Run project after setting up a topic
```bash
cd streamer
lein deps
lein run
```
