import pika
import json
import uuid

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="rabbitmq")
)

channel = connection.channel()

result = channel.queue_declare(
    queue="",
    exclusive=True
)

callback_queue = result.method.queue

request_id = str(uuid.uuid4())

request = {
    "well_id": "WELL-001",
    "works": [
        {
            "type": "drilling",
            "description": "Бурение скважины",
            "quantity": 100
        },
        {
            "type": "electrical",
            "description": "Монтаж электрооборудования",
            "quantity": 16
        },
        {
            "type": "logistics",
            "description": "Доставка оборудования",
            "quantity": 50
        }
    ]
}


def receive_estimate(ch, method, properties, body):
    estimate = json.loads(body)

    print("Итоговая смета по скважине")
    print("================================")

    for work in estimate["works"]:
        print("\nВид работ:", work["name"])
        print("Описание:", work["description"])
        print("Стоимость:", work["cost"], "руб.")
        print("Срок:", work["days"], "дней")

    print("\nОбщая стоимость:", estimate["total_cost"], "руб.")
    print("Общий срок:", estimate["total_days"], "дней")

    print("================================")

    ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.stop_consuming()


channel.basic_consume(
    queue=callback_queue,
    on_message_callback=receive_estimate
)

channel.basic_publish(
    exchange="",
    routing_key="repair.requests",
    body=json.dumps(request),
    properties=pika.BasicProperties(
        correlation_id=request_id,
        reply_to=callback_queue,
        delivery_mode=2
    )
)

print("Заявка отправлена!")
print("Номер заявки:", request_id)
print("Ожидаем расчёты исполнителей...")

channel.start_consuming()

connection.close()