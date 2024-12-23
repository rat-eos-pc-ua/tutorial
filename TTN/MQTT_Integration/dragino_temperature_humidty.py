import base64
import json
from datetime import datetime
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import os
import time
import subprocess  
import re
from dateutil import parser
import csv

# Retrieve configurations from environment variables
APP_NAME = os.getenv('USERNAME')
ACCESS_KEY = os.getenv('ACCESS_KEY_MQTT')
TIME_STRING = os.getenv('TIME_STRING')
DATA_TYPE = os.getenv('DATA_TYPE')
DEVICE_ID = os.getenv('DEVICE_ID')
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))

# Kafka configurations
KAFKA_TOPIC = 'rat-eos-pc'

def load_devices_from_csv(csv_file):
    devices = []
    with open(csv_file, mode='r') as file:
        csv_reader = csv.DictReader(file, delimiter=';') 
        for row in csv_reader:
            devices.append({
                'device_id': row['id'],
                'latitude': float(row['Latitude'].replace(',', '.')),  
                'longitude': float(row['Longitude'].replace(',', '.')),  
                'location': row['location']  
            })
    return devices


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker successfully")
        for device in devices:
            mqtt_topic = f"v3/{APP_NAME}/devices/{device['device_id']}/up"
            client.subscribe(mqtt_topic)  # Subscribing to the device topic dynamically
            print(f"Subscribed to topic: {mqtt_topic}")
    else:
        print(f"Failed to connect, return code {rc}")
        if rc == 5:
            print("Error: Authentication failure - check ACCESS_KEY and MQTT_BROKER configurations.")

def on_message(client, userdata, msg):
    try:
        message_payload = json.loads(msg.payload.decode())
        device_info = next((d for d in devices if f"v3/{APP_NAME}/devices/{d['device_id']}/up" == msg.topic), None)
        
        if not device_info:
            print("Received message from unknown device.")
            return
        
        transformed_data = transform_to_model(message_payload, device_info)  
        print("Transformed data:", transformed_data)

        kafka_producer.send(KAFKA_TOPIC, transformed_data)
        kafka_producer.flush()
        print("Data sent to Kafka")
        
    except Exception as e:
        print(f"Error processing message: {e}")

def transform_to_model(msg, device_info):
    try:
        if not msg:
            observation_data = {
                "id": device_info['device_id'],
                "dateObserved": datetime.now().isoformat(),
                "status": "outOfService",
                "source": "Dragino_Temperature_Humidity"
            }
            return [observation_data]  # Return a single placeholder

        uplink_message = msg.get('uplink_message', {})
        decoded_payload = uplink_message.get('decoded_payload', {})

        battery_voltage = decoded_payload.get('BatV', None) 
        temperature = float(decoded_payload.get('TempC_SHT', None))  
        humidity = float(decoded_payload.get('Hum_SHT', None))  

        # Determine the status based on the availability of the data fields
        if battery_voltage not in [None, 0] and temperature not in [None, 0] and humidity not in [None, 0]:
            status = 'working'
        else:
            status = 'withIncidence'

        geo_location = f"{device_info['latitude']},{device_info['longitude']}"

        # Build the transformed message using extracted data
        message = {
            'id': msg['end_device_ids']['device_id'], 
            'dateObserved': msg['received_at'],  
            'source': 'Dragino_Temperature_Humidity',  
            'status': [status],
            'dcPowerInput': battery_voltage,
            'temperature': temperature,
            'relativeHumidity': humidity,
            "location": {
                "type": "Point",
                "coordinates": geo_location,
                "description": device_info['location']
            },
        }
        
        print(f"Transformed data: {json.dumps(message)}")
        return message

    except Exception as e:
        print(f"Error processing message: {e}, original message: {msg}")
        return {'status': 'outOfService'}

# Function to load configuration from a JSON file
def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

def create_kafka_producer(retries=10, wait=30):
    config = load_config()
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['io.rateospc.pt:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                security_protocol=config['security_protocol'],
                sasl_mechanism=config['sasl_mechanism'],
                sasl_plain_username=config['sasl_plain_username'],
                sasl_plain_password=config['sasl_plain_password']
            )
            return producer
        except Exception as e:
            print(f"Attempt {i+1}/{retries} to create Kafka producer failed: {e}")
            time.sleep(wait)
    raise Exception("Failed to create Kafka producer after several attempts")


if __name__ == "__main__":
    print("Script Started")

    # Load device configurations from the CSV file
    devices = load_devices_from_csv('sensor_dragino_TH.csv')
    
    kafka_producer = create_kafka_producer()
    print("Kafka producer created")
    
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set("rat-eos-pc-1@ttn", ACCESS_KEY)
    
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message  # Global message handler for all devices
    
    print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

    mqtt_client.loop_forever()
