# Tutorial: Fetch Sensor Data from TTN Using MQTT Integration

## Step 1: Retrieve Data from Sensors Using TTN MQTT Integration

To start, integrate your sensor with [The Things Network (TTN)](https://www.thethingsnetwork.org/). Make sure that your sensor is configured correctly to send data to TTN.

Once the integration is set up, configure the **MQTT Integration** in the TTN console:

1. Log in to the TTN console.
2. Select your application.
3. Navigate to the **Integrations** section.
4. Add the **MQTT Integration** to enable MQTT-based data retrieval.

For detailed instructions, refer to the [TTN MQTT Integration Guide](https://www.thethingsindustries.com/docs/integrations/mqtt/).

---

## Prerequisites

- A `config.json` file with Kafka connection details.
- Python packages installed (`paho-mqtt`, `kafka-python`, etc.).
- TTN credentials set up as environment variables.

---

## Python Script: `example_mqttIntegration.py`

The provided Python script connects to the TTN MQTT broker to fetch sensor data. Note that TTN offers other integrations, but this example uses MQTT.

The script performs the following tasks:

### 1. **Connect to TTN via MQTT**:
- Uses the `paho.mqtt.client` library to establish a connection with the TTN MQTT broker.
- The MQTT broker URL, port, application name, and access key are specified.
- Subscribes to MQTT topics to receive sensor data (filtered by application and device ID).

### 2. **Parse and Transform Data**:
- Parses incoming MQTT messages and extracts sensor fields.
- Transforms the data into the required data model, as defined [here](https://www.rateospc.pt/swagger-ui/).
- Constructs a structured data model for each observation, including:
  - `dateObserved`: Timestamp of the observation.
  - `location`: Sensor location details such as ID, description, type, and geo-coordinates.
  - `source`: Source sensor type.
  - `relativeHumidity`: Relative humidity percentage.
  - `temperature`: Ambient temperature in Celsius.
  - `soilTemperature`: Soil temperature value.
  - `soilMoisture`: Soil moisture level.
  - `electricalConductivity`: Soil electrical conductivity in microsiemens per centimeter.
  
- Publishes the transformed data to a specified Kafka topic using a Kafka producer (e.g., `atnog-io.iot4fire.av.it.pt:9092`) and sends it to the topic `rat-eos-pc`.

The `on_message` callback method processes each MQTT message and sends only the latest sensor data.

---

## Configuring Environment Variables

Before running the script, you need to set up several environment variables to connect to the TTN MQTT broker and manage data flow. Here's a list of required variables:

- **APP_NAME**
  - **Description**: Your application name as registered in TTN.
  - **How to Retrieve**: Log in to the TTN console and find your application dashboard.

- **ACCESS_KEY**
  - **Description**: An MQTT access key allowing access to your application data.
  - **How to Retrieve**: 
    - Navigate to your application in the TTN console.
    - Go to the **API Keys** section.
    - Create or use an existing key with `Read` permissions and copy the access key.

- **MQTT_BROKER**
  - **Description**: The MQTT broker URL for TTN (default is `eu1.cloud.thethings.network`).
  - **How to Set**: Use the regional TTN broker (e.g., `eu1.cloud.thethings.network`).

- **MQTT_PORT**
  - **Description**: The port for MQTT communication (usually `1883` for non-secure and `8883` for secure connections).
  - **How to Set**: Typically set to `1883` or `8883`.

- **DEVICE_ID**
  - **Description**: Specific device ID for which data will be fetched.
  - **How to Retrieve**: 
    - In the TTN console, go to **Devices** under your application.
    - Select a device to find its `device_id`.

---

## Kafka Credentials

Ensure that your `config.json` file is populated with the correct Kafka credentials and configuration. This file should include fields such as:


