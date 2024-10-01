# Tutorial: Fetch Sensor Data from TTN Using Storage Integration

## Step 1: Retrieve Data from Sensors Using TTN Storage Integration

To begin, integrate your sensor with [The Things Network (TTN)](https://www.thethingsnetwork.org/). Ensure that your sensor is properly configured to send data to TTN.

Once integrated, activate the **Storage Integration** in the TTN console:

1. Navigate to the TTN console.
2. Select your application.
3. Go to the **Integrations** section.
4. Add the **Storage Integration** to store sensor data.

For more information, check [TTN Storage Integration Guide](https://www.thethingsindustries.com/docs/integrations/storage/enable/).

---

## Prerequisites

- A `config.json` file with Kafka connection details.
- Requirements installed (e.g., necessary Python packages).
- TTN credentials for environment variables.

---

## Python Script: `example_storageIntegration.py`

The provided Python script fetches sensor data from TTN via the **Storage Integration**. Note that TTN offers other integrations which can also be used.

The script performs the following tasks:

### 1. **Fetch Data from TTN**:
- Uses the `curl` command to retrieve data from the TTN storage API.
- The data is fetched based on the specified application name, access key, and time range, and can optionally be filtered by device ID (this information is available in the TTN console).

### 2. **Parse and Transform Data**:
- Parses the fetched data and transforms it into the required data model, as defined [here](https://atnog-iot4fire.av.it.pt/swagger-ui/).
- Extracts the relevant sensor data fields.
- Constructs a structured data model (based on the provided schema), which includes:
  - `dateObserved`: Timestamp of the observation.
  - `location`: Information about the sensor's location, including ID, description, type, and geo-coordinates (retrieved from TTN for the sensor).
  - `source`: Source sensor type.
  - `relativeHumidity`: Relative humidity value.
  - `temperature`: Temperature value.
  - `soilTemperature`: Soil temperature value.
  - `soilMoisture`: Soil moisture percentage.
  - `electricalConductivity`: Electrical conductivity of the soil in microsiemens per centimeter.
  
- Sends the transformed data to the specified Kafka topic using a Kafka producer (e.g., `atnog-io.iot4fire.av.it.pt:9092`) and the correct credentials for the topic `rat-eos-pc`.

The `get_most_recent_data` method ensures that only the most recent sensor data is sent.

---

## Configuring Environment Variables

Before running the script, you must configure several environment variables to fetch and manage data from TTN. Here’s how to retrieve and set these variables:

- **APP_NAME**
  - **Description**: The name of your application as registered in TTN.
  - **How to Retrieve**: Navigate to the TTN console and check the application dashboard; the application name is listed at the top.

- **ACCESS_KEY**
  - **Description**: The access key provides the necessary permissions to retrieve data via the API.
  - **How to Retrieve**: 
    - In the TTN console, select your application.
    - Go to the **API Keys** section.
    - Create a new API key (if necessary) with appropriate permissions (e.g., `Read data`), then copy the generated key.

- **TIME_STRING**
  - **Description**: This variable specifies the time range for the data you want to retrieve (e.g., '1h' for the last hour, '7d' for the last week).
  - **How to Set**: Use shorthand like `24h` for the last 24 hours or `30d` for the last 30 days.

- **DATA_TYPE**
  - **Description**: The type of data to fetch. Defaults to `uplink_message`.
  - **How to Set**: Leave as `uplink_message` or set another data type if required.

- **DEVICE_ID**
  - **Description**: Specifies a particular device’s data to fetch.
  - **How to Retrieve**: 
    - In the TTN console, navigate to the **Devices** section of your application.
    - Select the device to view its `device_id`.

---

## Kafka Credentials

Ensure that the `config.json` file contains the correct Kafka credentials and configuration. Update the following fields with the values given to you.

---

## Important Notes

- Adjust the script according to the specific sensors being used. Ensure that the correct sensor measurements are extracted and that the structured data model is followed accurately.
- Proper adherence to the data model is crucial for the correct functioning of Kibana visualizations.
