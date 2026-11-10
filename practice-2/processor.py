import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="rabbitmq")
)

channel = connection.channel()

channel.queue_declare(queue="repair.requests", durable=True)
channel.queue_declare(queue="repair.responses", durable=True)

channel.queue_declare(queue="work.drilling", durable=True)
channel.queue_declare(queue="work.electrical", durable=True)
channel.queue_declare(queue="work.logistics", durable=True)

routes = {
    "drilling": "work.drilling",
    "electrical": "work.electrical",
    "logistics": "work.logistics"
}

pending_requests = {}


def receive_request(ch, method, properties, body):
    data = json.loads(body)

    request_id = properties.correlation_id

    print("\nПолучена комплексная заявка:", request_id)

    works = data["works"]

    pending_requests[request_id] = {
        "reply_to": properties.reply_to,
        "expected": len(works),
        "results": {}
    }

    for work in works:
        work_type = work["type"]

        if work_type not in routes:
            print("Неизвестный вид работ:", work_type)
            continue

        queue = routes[work_type]

        print("Отправляем работу:", work_type)

        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(work),
            properties=pika.BasicProperties(
                correlation_id=request_id,
                reply_to="repair.responses",
                delivery_mode=2
            )
        )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def receive_result(ch, method, properties, body):
    result = json.loads(body)

    request_id = properties.correlation_id

    print("\nПолучен расчёт:", result)

    if request_id not in pending_requests:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    request = pending_requests[request_id]

    request["results"][result["type"]] = result

    if len(request["results"]) == request["expected"]:

        results = list(request["results"].values())

        total_cost = sum(item["cost"] for item in results)
        total_days = sum(item["days"] for item in results)

        estimate = {
            "request_id": request_id,
            "well_id": "WELL-001",
            "works": results,
            "total_cost": total_cost,
            "total_days": total_days
        }

        print("\nВсе расчёты получены!")
        print("Итоговая смета:", estimate)

        channel.basic_publish(
            exchange="",
            routing_key=request["reply_to"],
            body=json.dumps(estimate),
            properties=pika.BasicProperties(
                correlation_id=request_id,
                delivery_mode=2
            )
        )

        del pending_requests[request_id]

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(
    queue="repair.requests",
    on_message_callback=receive_request
)

channel.basic_consume(
    queue="repair.responses",
    on_message_callback=receive_result
)

print("Составной обработчик сообщений запущен!")

channel.start_consuming()