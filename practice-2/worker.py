import pika
import json
import os
import math

work_type = os.getenv("WORK_TYPE")

services = {
    "drilling": {
        "name": "Бурение",
        "price": 1500,
        "time_coefficient": 0.05
    },
    "electrical": {
        "name": "Электрика",
        "price": 5000,
        "time_coefficient": 0.15
    },
    "logistics": {
        "name": "Логистика",
        "price": 100,
        "time_coefficient": 0.2
    }
}

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="rabbitmq")
)

channel = connection.channel()

channel.queue_declare(queue="repair.responses", durable=True)

queue_name = "work." + work_type

channel.queue_declare(queue=queue_name, durable=True)


def process_work(ch, method, properties, body):
    data = json.loads(body)

    print("Получена заявка:", data)

    service = services[work_type]

    quantity = float(data["quantity"])

    cost = quantity * service["price"]

    days = math.ceil(quantity * service["time_coefficient"])

    result = {
        "type": work_type,
        "name": service["name"],
        "description": data["description"],
        "cost": cost,
        "days": days
    }

    print("Расчёт выполнен:", result)

    channel.basic_publish(
        exchange="",
        routing_key=properties.reply_to,
        body=json.dumps(result),
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
            delivery_mode=2
        )
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)


print("Сервис запущен:", work_type)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue=queue_name,
    on_message_callback=process_work
)

channel.start_consuming()
