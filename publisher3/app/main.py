import os
import random
import ssl
import sys

import pika
import time
import logging

from dotenv import load_dotenv

from publisher3.domain import Type3Event


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
    queue_name = Type3Event.__name__
    channel.queue_declare(queue=queue_name)

    # Configure logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    count = 1
    while True:
        event = Type3Event("event3", str({"message": f"Event3 - Message {count}"}))
        channel.basic_publish(exchange='', routing_key=queue_name, body=event.to_json())
        logging.info(f"Published: {event}")
        count += 1
        time.sleep(random.randint(5, 15))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Publisher stopped.")
        sys.exit(0)
