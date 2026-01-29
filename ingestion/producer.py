from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda value: json.dumps(value).encode("utf-8")) 

# send data to Kafka 

def send_reading(reading: dict) -> None:

    try:

        # Sending a simple string message
        producer.send('energy_readings', value=reading)
    
        # Ensure all messages are sent before exiting
        producer.flush()

    except Exception as e:
        print(f"Error sending message to Kafka: {e}")