import json 
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

load_dotenv()

def run_consumer():
    consumer = KafkaConsumer(
        "orders",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cur = conn.cursor()
    for message in consumer:
        order_data = message.value

        order_id = order_data['order_id'],
        status = order_data['status'],
        category = order_data['category'],
        value = order_data['value'],
        city = order_data['city'],
        payment_method = order_data['payment_method'],
        discount = order_data['discount']

        insert_query = """
             INSERT INTO orders (order_id, status, category, value, city, payment_method, discount)
             VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (order_id,status, category, value, city, payment_method, discount))
        conn.commit()
        print(f'The Consumer inserted order {order_id} into the database')
    
if __name__ == "__main__":
    run_consumer()
