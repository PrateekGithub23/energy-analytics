from kafka import KafkaConsumer
import json
from data_generator.storage import store_sensor

# Initialize Kafka consume
consumer = KafkaConsumer(
    "energy_readings",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="energy-lake-writer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print(" Kafka consumer started. Waiting for messages...")


# Consume messages continuously
# message is a Kafka message object
#message.value is the actual content of the message that is of the form of a dict
for message in consumer:
    reading = message.value
    store_sensor(reading)