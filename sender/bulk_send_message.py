from services.producer import Producer
from services.postgres import PostgresConnection
import time

def generate_data():
    progress = PostgresConnection(
        hostname="127.0.0.1",
        database="messages",
        username="yourusername",
        password="yourpassword",
    )
    progress.connect()
    for i in range(10):
        result = progress.execute_query("INSERT INTO messages (message, phone_number) VALUES (%s, %s) RETURNING id", (f"Process data {i}", f"+1234567890{i}"))
        if not result:
            continue
        yield result
    progress.close()

def main():
    producer = Producer("send_message_queue")
    for data in generate_data():
        message_id = str(data)
        print(f'[x] sending message id {message_id}')
        # Use direct exchange type to match worker configuration
        producer.send_message(message_id, exchange_type="direct")
        time.sleep(0.1)
    producer.close()

if __name__ == "__main__":
    main()
