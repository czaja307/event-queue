import os
import ssl
import sys
import time

import pika
import logging

from dotenv import load_dotenv

from consumer2.domain import Type2Event


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

    def callback(ch, method, properties, body):
        event = Type2Event.from_json(body)
        logging.info(f"Received: {event}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        time.sleep(7)

    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Consumer stopped.")
        sys.exit(0)
