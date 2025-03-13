import os
import ssl

import pika
import time
import logging

from dotenv import load_dotenv

from publisher1.domain import Type1Event

# Load env
load_dotenv()
CLOUDAMQP_URL = os.getenv("CLOUDAMQP_URL")

# Connect to RabbitMQ
params = pika.URLParameters(CLOUDAMQP_URL)
params.ssl_options = pika.SSLOptions(ssl.create_default_context())
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='event1')

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def publish_event():
    count = 1
    while True:
        event = Type1Event("event1", {"message": f"Event1 - Message {count}"})
        channel.basic_publish(exchange='', routing_key='event1', body=event.to_json())
        logging.info(f"Published: {event}")
        count += 1
        time.sleep(2)


try:
    publish_event()
except KeyboardInterrupt:
    logging.info("Publisher stopped.")
    connection.close()
