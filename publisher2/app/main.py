import os
import random
import ssl
import sys

import pika
import time
import logging

from dotenv import load_dotenv

from publisher2.domain import Type2Event


def main():
    # Load env
    load_dotenv()
    CLOUDAMQP_URL = os.getenv("CLOUDAMQP_URL")

    # Connect to RabbitMQ
    params = pika.URLParameters(CLOUDAMQP_URL)
    params.ssl_options = pika.SSLOptions(ssl.create_default_context())
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Declare the queue
    queue_name = Type2Event.__name__
    channel.queue_declare(queue=queue_name)

    # Configure logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    count = 1
    while True:
        event = Type2Event("event2", str({"message": f"Event2 - Message {count}"}))
        channel.basic_publish(exchange='', routing_key=queue_name, body=event.to_json())
        logging.info(f"Published: {event}")
        count += 1
        time.sleep(random.randint(1, 5))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Publisher stopped.")
        sys.exit(0)
