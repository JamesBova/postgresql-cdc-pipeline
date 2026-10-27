import base64
from decimal import Decimal
import json
from confluent_kafka import Consumer, KafkaException
import psycopg
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

def decode_balance(value):
    if value is None:
        return None

    raw_bytes = base64.b64decode(value)
    unscaled = int.from_bytes(raw_bytes, byteorder="big", signed=True)

    return Decimal(unscaled).scaleb(-2)

db = psycopg.connect(
    host="127.0.0.1",
    port=55433,
    dbname="target_db",
    user="cdc_user",
    password=os.environ["CDC_DB_PASSWORD"]
)

def apply_change(operation, before, after):
    with db.transaction():
        with db.cursor() as cursor:
            if operation in ("r", "c", "u"):
                cursor.execute(
                    """
                    INSERT INTO customers
                        (customer_id, customer_name, balance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (customer_id)
                    DO UPDATE SET
                        customer_name = EXCLUDED.customer_name,
                        balance = EXCLUDED.balance
                    """,
                    (
                        after["customer_id"],
                        after["customer_name"],
                        after["balance"]
                    )
                )

            elif operation == "d":
                cursor.execute(
                    "DELETE FROM customers WHERE customer_id = %s",
                    (before["customer_id"],)
                )

consumer = Consumer({
    "bootstrap.servers": "127.0.0.1:9094",
    "group.id": "cdc-python-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe(["cdc.public.customers"])

print("Waiting for CDC events...")

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            raise KafkaException(message.error())

        # Kafka tombstone records have a null value.
        if message.value() is None:
            consumer.commit(message=message, asynchronous=False)
            continue

        event = json.loads(message.value().decode("utf-8"))
        
        payload = event.get("payload") or {}

        operation = payload.get("op")
        before = payload.get("before")
        after = payload.get("after")

        if after:
            after["balance"] = decode_balance(after["balance"])

        if operation in ("r", "c", "u", "d"):
            apply_change(operation, before, after)
            consumer.commit(message=message, asynchronous=False)

        print(f"Operation: {operation}")
        print(f"Before: {before}")
        print(f"After: {after}")
        print("-" * 40)

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    db.close()
    consumer.close()
