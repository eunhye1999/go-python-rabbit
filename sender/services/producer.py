import pika

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

class Producer:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)  # Ensure queue exists

    def send_message(self, message: str):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2),  # Persistent message
        )
        print(f" [x] Sent: {message}")

    def close(self):
        """Closes the RabbitMQ connection."""
        self.connection.close()
