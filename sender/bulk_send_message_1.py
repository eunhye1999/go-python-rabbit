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
    for i in range(500):
        result = progress.execute_query("INSERT INTO messages (message, phone_number) VALUES (%s, %s) RETURNING id", (f"Process data {i} (2)", f"+1234567890{i}"))
        if not result:
            continue
        yield result
    progress.close()

def main():
    producer = Producer("send_message")
    for data in generate_data():
        print(f'[x] sending message id {data}')
        producer.send_message(f'{data}')
        time.sleep(0.1)
    producer.close()

if __name__ == "__main__":
    main()
