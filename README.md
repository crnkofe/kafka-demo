# Kafka Demo

Demonstrates writing to and processing simple timeseries data.

Producer generates a double sinusoid.

Consumer calculates a first-order derivative.

## Setup

Install virtualenvwrapper, docker, docker-compose.

Create a virtual env:
```bash
mkvirtualenv --python=/usr/bin/python3 kafka-demo
```

Install requirements:
```bash
pip install --requirements requirements.txt
```

Starto docker!
```bash
docker-compose up -d
```

Run script.
```bash
python demo.py
```
