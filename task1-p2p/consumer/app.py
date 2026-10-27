import os
import time

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "repair_requests"
BRIGADE_NAME = os.getenv("BRIGADE_NAME", "brigade-unknown")

connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)
channel.basic_qos(prefetch_count=1)


def handle_message(ch, method, properties, body):
    print(f"[{BRIGADE_NAME}] получила: {body.decode()}")
    time.sleep(3)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[{BRIGADE_NAME}] закончила работу")


channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
print(f"[{BRIGADE_NAME}] ждёт заявки...")
channel.start_consuming()