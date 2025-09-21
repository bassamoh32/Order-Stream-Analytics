import time
import json
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker 

fake = Faker()

def generate_synthetics_orders():
    """
    Generate synthetic e-commerce order data
    """
    categorises = ["Soccer", "Basketball", "Running", "Swiming", "Cycling", "Volleyball", "Tennis"]
    statuses = ["Processing", "Completed", "Cancelled"]
    cities = ["Agadir", "Rabat", "Casablanca", "Tanger", "Asfi", "Marrakech"]
    payment_methods = [
    "Credit Card",
    "Debit Card",
    "PayPal",
    "Apple Pay",
    "Google Pay",
    "Bank Transfer",
    "Cash",
    "Cryptocurrency"]
    discounts = [0, 0.05, 0.10, 0.15]

    category = fake.random.choice(categorises)
    status = fake.random.choice(statuses)
    city = fake.random.choice(cities)
    payment_method = fake.random.choice(payment_methods)
    discount = fake.random.choice(discounts)

    gross_value = fake.pyfloat(min_value=50, max_value=200, right_digits=2)
    net_value = gross_value * (1 - discount)

    return {
        "order_id": str(uuid.uuid4())[:8],
        "status": status,
        "category": category,
        "value": round(net_value, 2),
        "timestamp": datetime.now().strftime('%Y-%m-%d'),
        "city": city,
        "payment_method": payment_method,
        "discount": round(discount, 2)
    }


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    while True:
        order = generate_synthetics_orders()
        print(f'Sending order : {order}')
        try:
            producer.send('orders', value = order)
            print('The order has been send to kafka topic')
            producer.flush()
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")
            logger.error(f"Failed to send message to Kafka: {e}")
        time.sleep(2)

if __name__ == "__main__" :
    run_producer()
