import pika

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

class Producer:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)  # Ensure queue exists

    # def send_message(self, message: str):
    #     self.channel.basic_publish(
    #         exchange="",
    #         routing_key=self.queue_name,
    #         body=message,
    #         properties=pika.BasicProperties(delivery_mode=2),  # Persistent message
    #     )
    #     print(f" [x] Sent: {message}")

    def close(self):
        """Closes the RabbitMQ connection."""
        self.connection.close()


    def _get_exchange_and_routing_key(self, exchange_type: str) -> tuple:
        if exchange_type == "direct":
            return "default_exchange", self.queue_name
        elif exchange_type == "headers":
            return "headers_exchange", ""  # Headers exchange does not need a routing key
        else:
            return "", "default_routing_key"

    def _validate_exchange_type(self, exchange_type: str) -> str:
        valid_exchange_types = ["direct", "topic", "headers", "fanout"]
        return exchange_type if exchange_type in valid_exchange_types else "direct"

    def send_message(self, message: str, exchange_type: str = "direct", headers: dict = None):
        exchange, routing_key = self._get_exchange_and_routing_key(exchange_type)
        exchange_type = self._validate_exchange_type(exchange_type)

        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)

        if exchange_type == "headers" and headers is not None:
            # Set headers for routing in a headers exchange
            properties = pika.BasicProperties(
                delivery_mode=2,  # Persistent message
                headers=headers,   # Set the headers for the message
            )
        else:
            # For other types of exchanges (direct, topic, etc.)
            properties = pika.BasicProperties(delivery_mode=2)  # Persistent message

        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=properties
        )
        print(f" [x] Sent: {message}")