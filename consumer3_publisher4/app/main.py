import os
import ssl
import sys
import time

import pika
import logging

from dotenv import load_dotenv

from consumer3_publisher4.domain import Type3Event, Type4Event


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
    incoming_queue_name = Type3Event.__name__ + "Queue"
    channel.queue_declare(queue=incoming_queue_name)

    outgoing_queue_name = Type4Event.__name__ + "Queue"
    channel.queue_declare(queue=outgoing_queue_name)

    # Configure logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    count = 1

    def callback(ch, method, properties, body):
        nonlocal count
        event = Type3Event.from_json(body)
        logging.info(f"Received: {event}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

        time.sleep(2)

        new_event = Type4Event("event4", str({"message": f"Event4 - Message {count}"}))
        channel.basic_publish(exchange='', routing_key=outgoing_queue_name, body=new_event.to_json())
        logging.info(f"Published: {new_event}")
        count += 1

    channel.basic_consume(queue=incoming_queue_name, on_message_callback=callback)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Consumer stopped.")
        sys.exit(0)
