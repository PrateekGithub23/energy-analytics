import random
from datetime import datetime
import time
from data_generator.storage import store_sensor
from ingestion.producer import send_reading


def generate_sensor_data(sensor_id: str) -> dict:
    data = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "energy_kwh": round(random.uniform(0.2, 2.5), 2),
        "voltage": random.randint(200, 240),
        "room_temp": round(random.uniform(18.0, 30.0), 1),
    }
    return data

numSensors = int(input("Enter number of sensors to generate data for: "))
numReadings = int(input("Enter number of readings per sensor: "))

# Sensor IDs generation and populating the sensors list
sensors = []
for i in range(numSensors):
    sensor_id = f"SENSOR_{i+1:03d}"
    sensors.append(sensor_id)

# Generate readings for each sensor, store in a dictionary, where keys are sensor IDs and values are lists of readings (dicts)
def generate_readings():

    # Generates numSensors x NumReadings sensor readings and stores them using store_sensor function
    for sensor in sensors:
        # for each sensor, generate numReadings readings
        for j in range(numReadings):
            reading = generate_sensor_data(sensor) # reading is a dict
            send_reading(reading)
            time.sleep(0.1)
    

generate_readings()
    
""" for testing purposes, return the generated data as a dictionary where keys are sensor IDs and values are lists of readings
    sensor_data = {}
    for sensor in sensors:
        sensor_data[sensor] = []
        for j in range(numReadings):
            reading = generate_sensor_data(sensor)
            sensor_data[sensor].append(reading)
            time.sleep(0.1)  # Simulate delay for realism

    return sensor_data """

    



# Write to output.json
"""
with open("output.json", "w") as f:
    json.dump(sensor_data, f, indent=4)

print("Sensor data written to output.json")""" 