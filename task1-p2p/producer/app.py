import os
import time

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = "repair_requests"

connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)

wells = [12, 7, 34, 5, 21, 8, 45, 3]

for i, well in enumerate(wells, start=1):
    message = f"Заявка #{i}: ремонт скважины №{well}"
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"[dispatcher] отправлена {message}")
    time.sleep(2)

connection.close()