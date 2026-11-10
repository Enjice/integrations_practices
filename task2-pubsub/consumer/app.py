import os

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
EXCHANGE_NAME = "price_updates"
DEPARTMENT = os.getenv("DEPARTMENT", "unknown")

connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout")

result = channel.queue_declare(queue="", exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)


def handle_message(ch, method, properties, body):
    print(f"[{DEPARTMENT}] получил: {body.decode()}")


channel.basic_consume(queue=queue_name, on_message_callback=handle_message, auto_ack=True)
print(f"[{DEPARTMENT}] подписан на {EXCHANGE_NAME}, очередь {queue_name}")
channel.start_consuming()