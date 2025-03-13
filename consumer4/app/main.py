import os
import ssl
import sys
import time

import pika
import logging

from dotenv import load_dotenv

from consumer4.domain import Type4Event


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
    channel.queue_declare(queue=Type4Event.__name__ + "Queue")

    # Configure logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def callback(ch, method, properties, body):
        event = Type4Event.from_json(body)
        logging.info(f"Received: {event}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        time.sleep(13)

    channel.basic_consume(queue='event1', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Consumer stopped.")
        sys.exit(0)
