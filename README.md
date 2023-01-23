# kafka-python

Python client applications which produce and consume messages from an Apache Kafka cluster.

#### Requirements
- python3.8+
- docker
- poetry
- confluent-kafka

#### Set up a Kafka broker with docker
- [kafka-docker](https://developer.confluent.io/quickstart/kafka-docker/)

#### Setup project with `poetry`
```
$ source scripts/setup
$ poetry shell
$ producer getting_started.ini
$ consumer getting_started.ini
```

(OR)
#### Setup project with `venv`
```
$ python3.8 -m venv .venv
$ source .venv/bin/activate
$ python -m pip install --upgrade pip
$ python -m pip install .
$ producer getting_started.ini
$ consumer getting_started.ini
```
