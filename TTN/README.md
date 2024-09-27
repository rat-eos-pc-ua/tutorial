## Step 1: Retrieve Data from Sensors Using the TTN STORAGE INTEGRATION

To start, integrate your sensor with The Things Network (TTN). Ensure that your sensor is properly configured to send data to TTN.

After integrating the sensor, activate the storage integration on the TTN console:
1. Navigate to the TTN console.
2. Select your application.
3. Go to the **Integrations** section.
4. Add the **Storage Integration** to store the sensor data.
   

### Python Script
The following Python script available  is an example that fetches sensor data from TTN via the storage integration. Keep in mind that are other integrations available in TTN that can be used.
The script performs the following tasks:

1. **Fetch Data from TTN**:
    - Uses the `curl` command to retrieve data from the TTN storage API.
    - The data is fetched based on the specified application name, access key, and time string, and can optionally be filtered by device ID. This information is accesible in TTN console. 

2. **Parse and Transform Data**:
    - Parses the fetched data and transforms it into the required data model. The data model that needs to be followed can be seen here: [here]( https://atnog-iot4fire.av.it.pt/swagger-ui/)
    - Extracts important sensor data fields such as `temperature`, `humidity`,  `co2` or other fields relevant from the decoded payload.
    - Constructs a structured data model that includes:
        - `dateObserved`: The timestamp of the observation.
        - `location`: Information about the sensor's location, including its ID, description, type, and geo-coordinates.
        - `relativeHumidity`: The relative humidity value.
        - `temperature`: The temperature value.
        - `soilTemperature`: The soil temperature value.
        - `soilMoisture`: The soil moisture value in percentage.
        - `electricalConductivity`: Electrical conductivity of the soil in microsiemens per centimeter
        - `source`: A string indicating the source sensor type.
    - Sends the transformed data to the specified Kafka topic using a Kafka producer at the specified address (e.g., `atnog-io.iot4fire.av.it.pt:9092`) and the topic "rat-eos-pc"
          
### Important Notes
It is crucial to adjust the values in the script according to the specific sensors being used. This includes ensuring the correct extraction of sensor measurements and maintaining the structured data model. 
Proper adherence to this data model is essential for the correct functioning of Kibana visualizations.

