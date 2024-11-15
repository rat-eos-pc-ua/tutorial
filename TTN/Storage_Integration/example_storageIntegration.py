import base64
import json
from datetime import datetime
from kafka import KafkaProducer
import os
import time
import subprocess  
import re
from dateutil import parser


class Error(Exception):
    """Base class for errors in this module."""
    pass

class FetchError(Error):
    """Raised when sensor_pull_storage can't deal with input.

    Attributes:
       expression -- input expression where error occurred.
       message -- explanation of the error.
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

#retrieve info from storage integration
def sensor_pull_storage(appname, accesskey, timestring, data_type="uplink_message", device_id=None, *, data_folder=None, ttn_version=3):
    """
    Pull data from TTN via the TTN storage API.

    Parameters:
        appname (str): The name of the TTN app.
        accesskey (str): The full accesskey from TTN.
        timestring (str): Indicates the amount of data needed, e.g., '100h'.
        data_type (str): Type of the data to fetch, defaults to 'uplink_message'.
        device_id (str): Specific device ID to fetch data for, defaults to None.
        data_folder (str or Path): If provided, the directory where the output
            file should be stored. Defaults to None.
        ttn_version (int): The TTN version. Should be 2 or 3; 3 is the default.

    Returns:
        list or str: If data_folder is None, returns the data as a Python array
        (for V3) or a string (for V2). Otherwise, writes the data to a file and
        returns None.
    """
    base_url = f"https://eu1.cloud.thethings.network/api/v3/as/applications/{appname}"
    if device_id:
        url = f"{base_url}/devices/{device_id}/packages/storage/{data_type}"
    else:
        url = f"{base_url}/packages/storage/{data_type}"

    args = [
        "/usr/bin/curl",
        "-G", url,
        "--header", f"Authorization: Bearer {accesskey}",
        "--header", "Accept: text/event-stream",
        "-d", f"last={timestring}"
    ]

    if data_folder is not None:
        tFolder = pathlib.Path(data_folder)
        if not tFolder.is_dir():
            raise FetchError(f"data_folder={data_folder}", "Not a directory")
        args += ["-o", str(tFolder / f"{data_type}_lastperiod.json")]

    try:
        result = subprocess.run(args, shell=False, check=True, capture_output=True)
        if ttn_version == 3:
            return list(map(json.loads, re.sub(r'\n+', '\n', result.stdout.decode()).splitlines()))
        else:
            return result.stdout
    except subprocess.CalledProcessError as e:
        raise FetchError("Error executing curl command", str(e)) from e


def parse_datetime_with_nanoseconds(date_string):
    """Convert ISO8601 date string with potentially nanoseconds to a datetime object using dateutil."""
    return parser.parse(date_string)

#method to get only the last data measurements
def get_most_recent_data(data_entries):
    """Returns only the data entry with the most recent observation date."""
    if not data_entries:
        return None
    
    most_recent_entry = data_entries[0]
    most_recent_time = parse_datetime_with_nanoseconds(most_recent_entry['result']['received_at'])
    
    for entry in data_entries[1:]:
        current_time = parse_datetime_with_nanoseconds(entry['result']['received_at'])
        if current_time > most_recent_time:
            most_recent_time = current_time
            most_recent_entry = entry
    
    return most_recent_entry


def getzf(c_num):
    return f"{c_num:02d}"

# Function to get formatted date
def getMyDate(timestamp):
    if isinstance(timestamp, str) and len(timestamp) > 10:
        return timestamp
    if len(str(timestamp)) > 10:
        timestamp = int(timestamp)
    else:
        timestamp = int(timestamp) * 1000

    c_Date = datetime.fromtimestamp(timestamp / 1000)
    c_Time = c_Date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return c_Time


# Function to send data to Kafka
def send_data(producer, topic, message):
    producer.produce(topic, value=json.dumps(message))
    producer.flush()  # Ensure data is sent to Kafka
    print(f"Sent data to Kafka: {message}")

# Function to process messages and transform to data model
def transform_to_model(msg, status='working'):
    try:

        if 'result' not in msg or 'end_device_ids' not in msg['result']:
            print("Message format is incorrect or missing required data")
            return None

        device_info = msg['result']['end_device_ids']
        if 'device_id' not in device_info:
            print("Device ID is missing from the message")
            return None

        device_id = device_info['device_id']

        # Extracting the decoded payload 
        uplink_message = msg['result'].get('uplink_message', {})
        decoded_payload = uplink_message.get('decoded_payload', {})

        # Check for fields for correct decoding, if any field is missing, set sensor status to 'withIncidence'
        required_fields = ['BatV', 'temp_DS18B20', 'temp_SOIL', 'water_SOIL', 'conduct_SOIL']
        for field in required_fields:
            if field not in decoded_payload:
                status = 'withIncidence'
                print(f"Field '{field}' is missing in the decoded payload")

        #get coordinates from TTN
        geo_location = "Unknown location"
        for metadata in msg['result']['uplink_message']['rx_metadata']:
            if 'location' in metadata:
                location_info = metadata['location']
                geo_location = f"{location_info['latitude']},{location_info['longitude']}"
                break

        # Constructing the message with directly extracted data
        message = {
            'id': device_id,
            'dateObserved': getMyDate(msg['result']['received_at']),
            'source': 'Dragino_soil',
            'status': [status],  
            'dcPowerInput': decoded_payload.get('BatV', 0),  
            'temperature': decoded_payload.get('temp_DS18B20', '0.00'),
            'soilTemperature': decoded_payload.get('temp_SOIL', '0.00'),
            'soilMoisture': decoded_payload.get('water_SOIL', '0.00'),
            'electricalConductivity': decoded_payload.get('conduct_SOIL', 0),
            "location": {
                "type": "Point",
                "coordinates": geo_location,
                "description": 'Aveiro'
            },
        }

        print(f"Received message: {json.dumps(msg, indent=2)}")
        print(f"Decoded data: {json.dumps(message, indent=2)}")
        return message
    except Exception as e:
        print(f"Error processing message: {e}, original message: {msg}")
        return None

# Function to load configuration from a JSON file
def load_config():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
            print("Config loaded successfully: ", config)  # Debug statement
            return config
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
        raise
    except FileNotFoundError as e:
        print(f"Configuration file not found: {e}")
        raise

# Create the Kafka producer using the loaded configuration
def create_producer(retries=10, wait=30):
    """Attempt to create a Kafka producer with retries """
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config['bootstrap_servers'],
                # security_protocol=config['security_protocol'],
                # sasl_mechanism=config['sasl_mechanisms'],
                # sasl_plain_username=config['sasl_username'],
                # sasl_plain_password=config['sasl_password'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            return producer
        except Exception as e:
            print(f"Attempt {i+1}/{retries} failed: {e}")
            time.sleep(wait) # wait 30 seconds before try again
    raise Exception("Failed to create Kafka producer after several attempts")


if __name__ == "__main__":
    
    #informations from TTN
    appname = os.getenv('APP_NAME')
    accesskey = os.getenv('ACCESS_KEY')
    timestring = os.getenv('TIME_STRING')
    data_type = os.getenv('DATA_TYPE', 'uplink_message')
    device_id = os.getenv('DEVICE_ID')
    config= load_config()
    producer = create_producer()
    print("Producer created")

    while True:
        try:
            all_data = sensor_pull_storage(appname, accesskey, timestring, data_type, device_id)
            if all_data:
                most_recent_data = get_most_recent_data(all_data)
                if most_recent_data:
                    # If we get the most recent data the sensor is working
                    transformed_data = transform_to_model(most_recent_data, status='working')
                    print(transformed_data)
                    producer.send('rat-eos-pc', transformed_data)
                    producer.flush()
                    print("Data sent")
                else:
                    print("No recent data found.")

            else:
                # If no data received at all, set status to outOfService 
                transformed_data = transform_to_model({}, status='outOfService')
                print("No data received.")
                producer.send('rat-eos-pc', transformed_data)
                producer.flush()

        except FetchError as fe:
            # If there is an error in fetching data, treat it as withIncidence
            print(f"Fetch error: {fe}")
            transformed_data = transform_to_model({}, status='withIncidence')
            producer.send('rat-eos-pc', transformed_data)
            producer.flush()

        time.sleep(3600)