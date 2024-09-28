## Step 1: Retrieve Data from Sensors Using the TTN STORAGE INTEGRATION

To start, integrate your sensor with The Things Network (TTN). Ensure that your sensor is properly configured to send data to TTN.

After integrating the sensor, activate the storage integration on the TTN console:
1. Navigate to the TTN console.
2. Select your application.
3. Go to the **Integrations** section.
4. Add the **Storage Integration** to store the sensor data.
   
For more information you can check: [https://www.thethingsindustries.com/docs/integrations/storage/enable/](https://www.thethingsindustries.com/docs/integrations/storage/enable/) 

## Prerequisites

- Docker installed
- Docker Compose installed
- A`config.json` file with our Kafka connection details

### Python Script
The following Python script available  is an example that fetches sensor data from TTN via the storage integration. Keep in mind that are other integrations available in TTN that can be used.
The script performs the following tasks:

1. **Fetch Data from TTN**:
    - Uses the `curl` command to retrieve data from the TTN storage API.
    - The data is fetched based on the specified application name, access key, and time string, and can optionally be filtered by device ID. This information is accesible in TTN console. 

2. **Parse and Transform Data**:
    - Parses the fetched data and transforms it into the required data model. The data model that needs to be followed can be seen here: [here]( https://atnog-iot4fire.av.it.pt/swagger-ui/)
    - Extract the important sensor data fields.
    - Constructs a structured data model that includes:
        - `dateObserved`: The timestamp of the observation.
        - `location`: Information about the sensor's location, including its ID, description, type, and geo-coordinates (retreive the coordinates that are set on TTN for the sensor).
        - `source`: A string indicating the source sensor type.
        - `relativeHumidity`: The relative humidity value.
        - `temperature`: The temperature value.
        - `soilTemperature`: The soil temperature value.
        - `soilMoisture`: The soil moisture value in percentage.
        - `electricalConductivity`: Electrical conductivity of the soil in microsiemens per centimeter
    - Sends the transformed data to the specified Kafka topic using a Kafka producer at the specified address (e.g., `atnog-io.iot4fire.av.it.pt:9092`)  with the correct credentials the topic "rat-eos-pc"

The method “get_most_recent_data” was made to to send only the most recent values that the sensor has collected: 


### Important Notes
It is crucial to adjust the values in the script according to the specific sensors being used. This includes ensuring the correct extraction of sensor measurements and maintaining the structured data model. 
Proper adherence to this data model is essential for the correct functioning of Kibana visualizations.

### Configuring Environment Variables
Before running the script, you must configure several environment variables that the script uses to fetch and manage data from The Things Network (TTN). Here’s how to retrieve and set these variables:

1. APP_NAME
   
The name of your application as registered in TTN.
How to Retrieve:
Go to the TTN console.
Navigate to your application dashboard.
The application name is listed at the top of the dashboard.

3. ACCESS_KEY
   
The access key provides the necessary permissions to retrieve data via the API.
How to Retrieve:
In the TTN console, select your application.
Go to the API keys section.
If you don’t have an existing key, create a new API key with appropriate permissions (e.g., Read data).
Copy the generated key.

5. TIME_STRING

This variable specifies the time range for the data you want to retrieve (e.g., '1h' for the last hour, '7d' for the last week).
How to Set:
Determine the time interval relevant to your needs.
Use shorthand like 24h

5. DATA_TYPE

Type of data to fetch; defaults to uplink_message.

6. DEVICE_ID

Specifies a particular device’s data to fetch; 
How to Retrieve:
If you need data from a specific device, navigate to the Devices section in your TTN application click on the device to check his id.
