import json
from datetime import datetime
from kafka import KafkaProducer
import paho.mqtt.client as mqtt
import os
import time
import csv

# Retrieve configurations from environment variables
APP_NAME = os.getenv('USERNAME')
ACCESS_KEY = os.getenv('ACCESS_KEY_MQTT')
DEVICE_ID = os.getenv('DEVICE_ID')
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))

# Kafka configurations
KAFKA_TOPIC = 'rat-eos-pc'

# Topic for MQTT subscription based on provided format
MQTT_TOPIC = f"v3/{APP_NAME}/devices/{DEVICE_ID}/up"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker successfully")
        client.subscribe(MQTT_TOPIC)  # Subscribing to the specified topic
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}")
        if rc == 5:
            print("Error: Authentication failure - check ACCESS_KEY and MQTT_BROKER configurations.")

def on_message(client, userdata, msg):
    try:
        message_payload = json.loads(msg.payload.decode())
        
        if not message_payload:
            print("Received empty message payload.")
            return
        print(message_payload)
        transformed_data = transform_to_model(message_payload)

        kafka_producer.send(KAFKA_TOPIC, transformed_data)
        kafka_producer.flush()
        print("Data sent to Kafka")
        
    except Exception as e:
        print(f"Error processing message: {e}")


def load_devices_from_csv(csv_file):
    devices = []
    with open(csv_file, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            devices.append({
                'device_id': row['Device ID'],
                'latitude': float(row['Latitude']),
                'longitude': float(row['Longitude']),
                'location': row['location']
            })
    return devices


# Function to process messages and transform to data model
def transform_to_model(msg,device_info):
    try:
        print(msg)
        if not msg:
            observation_data = {
                "id": device_info['Device ID'],
                "dateObserved": datetime.now().isoformat(),
                "status": 'outOfService',
                "source": "Dragino_soil"
            }
            return observation_data  # Returns a single placeholder
       # Extracting relevant data
        transformed_data = []
        end_device_ids = msg.get('end_device_ids', {})
        uplink_message = msg.get('uplink_message', {})
        decoded_payload = uplink_message.get('decoded_payload', {})
        received_at = msg.get('received_at', datetime.now().isoformat())

        # Check if end_device_ids is missing
        if not end_device_ids:
            raise KeyError("Missing 'end_device_ids' in the message data.")

        # Check if uplink_message is missing
        if not uplink_message:
            raise KeyError("Missing 'uplink_message' in the message data.")

        # Check if decoded_payload is missing
        if not decoded_payload:
            raise KeyError("Missing 'decoded_payload' in the uplink message.")


        # Get the necessary fields from the decoded payload
        battery_voltage = decoded_payload.get('BatV')
        temperature = float(decoded_payload.get('temp_DS18B20', None))  
        soil_temperature = float(decoded_payload.get('temp_SOIL', None))
        soil_moisture = float(decoded_payload.get('water_SOIL', None))
        electrical_conductivity = float(decoded_payload.get('conduct_SOIL', None))

        # Determine sensor status
        status = 'working' if all(v not in [None, 0] for v in 
                                   [temperature, battery_voltage, soil_temperature, soil_moisture, electrical_conductivity]) else 'withIncidence'

        geo_location = f"{device_info['latitude']},{device_info['longitude']}"


        # Constructing the message with directly extracted data
        message = {
            'id': end_device_ids['device_id'],
            'dateObserved': received_at,  # use the received_at field
            'source': 'Dragino_soil',
            'status': [status],  
            'dcPowerInput': battery_voltage,  
            'soilTemperature': soil_temperature,
            'soilMoisture': soil_moisture,
            'electricalConductivity': electrical_conductivity,
            "location": {
                "type": "Point",
                "coordinates": geo_location,
                "description": device_info['location']
            },
        }

        print(f"Received message: {json.dumps(msg, indent=2)}")
        print(f"Decoded data: {json.dumps(message, indent=2)}")

        transformed_data.append(message)

        return message

    except KeyError as e:
        print(f"Key error: {e} in message: {msg}")
    except Exception as e:
        print(f"Error processing message: {e}, original message: {msg}")
    return None

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
    devices = load_devices_from_csv('sensor_dragino_soil.csv')
    
    kafka_producer = create_kafka_producer()
    print("Kafka producer created")
    
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set("rat-eos-pc-1@ttn", ACCESS_KEY)
    
    # Define on_message callback to handle device-specific transformation
    def on_message_wrapper(device_info):
        def on_message(client, userdata, msg):
            try:
                message_payload = json.loads(msg.payload.decode())
                if not message_payload:
                    print("Received empty message payload.")
                    return
                print(message_payload)
                
                # Pass device_info to transformation function
                transformed_data = transform_to_model(message_payload, device_info)

                kafka_producer.send(KAFKA_TOPIC, transformed_data)
                kafka_producer.flush()
                print(f"Data sent to Kafka for device {device_info['device_id']}")
                
            except Exception as e:
                print(f"Error processing message for {device_info['device_id']}: {e}")
        return on_message

    # Loop through each device from CSV and set up MQTT client
    for device in devices:
        DEVICE_ID = device['device_id']
        MQTT_TOPIC = f"v3/{APP_NAME}/devices/{DEVICE_ID}/up"

        mqtt_client.on_connect = on_connect
        mqtt_client.message_callback_add(MQTT_TOPIC, on_message_wrapper(device))
        
        print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT} for device {DEVICE_ID}...")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    
    mqtt_client.loop_forever()