import os
import time

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
EXCHANGE_NAME = "price_updates"

connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="fanout")

prices = [78.4, 78.9, 79.3, 78.6, 80.1, 80.5, 79.8, 81.2]

for i, price in enumerate(prices, start=1):
    message = f"Изменение #{i}: цена барреля = {price} USD"
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key="",
        body=message,
    )
    print(f"[trader] опубликовано: {message}")
    time.sleep(3)

connection.close()