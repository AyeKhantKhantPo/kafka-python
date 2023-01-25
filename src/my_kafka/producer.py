import json

import structlog
from kafka import KafkaProducer

log = structlog.get_logger()


def main():
    # create a producer. broker is running on localhost
    producer = KafkaProducer(retries=5, bootstrap_servers=["localhost:9092"])
    # define the on success and on error callback functions

    def on_success(record):
        print(record.topic)
        print(record.partition)
        print(record.offset)

    def on_error(excp):
        log.error(excp)
        raise Exception(excp)

    # send the message to fintechexplained-topic
    producer.send(
        "my-topic", json.dumps({"key": "value"}).encode("utf-8")
    ).add_callback(on_success).add_errback(on_error)
    # block until all async messages are sent
    producer.flush()


if __name__ == "__main__":
    main()
