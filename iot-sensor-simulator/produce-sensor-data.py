from kafka import KafkaProducer
import random
import time
import json

# Define the Redpanda topic and Kafka producer
topic = "temperature_sensor"
producer = KafkaProducer(bootstrap_servers='localhost:19092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Simulate temperature readings
while True:
    # Generate a random temperature reading between 0°C and 100°C
    temperature = random.uniform(0, 100)

    # Create a JSON payload with a timestamp and temperature value
    data = {
        "timestamp": int(time.time()),
        "temperature": temperature
    }

    # Send the data to the Redpanda topic
    producer.send(topic, value=data)
    print(f"Sent data: {data}")

    # Simulate data every 5 seconds (adjust the interval as needed)
    time.sleep(5)
