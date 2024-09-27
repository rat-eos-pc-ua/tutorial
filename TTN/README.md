## Step 1: Retrieve Data from Sensors Using the TTN STORAGE INTEGRATION

To start, integrate your sensor with The Things Network (TTN). Ensure that your sensor is properly configured to send data to TTN.

After integrating the sensor, activate the storage integration on the TTN console:
1. Navigate to the TTN console.
2. Select your application.
3. Go to the **Integrations** section.
4. Add the **Storage Integration** to store the sensor data.
   

### Python Script
The following Python script is an example that fetches sensor data from TTN via the storage API: [here](https://github.com/rat-eos-pc-ua/ingress/blob/main/sensor/app.py)

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
        - `windDirection` and `windSpeed`: Currently set to `None` (can be adjusted based on sensor data).
        - `co2`: The CO2 value.
        - `source`: A string indicating the source sensor type.
    - Sends the transformed data to the specified Kafka topic using a Kafka producer at the specified address (e.g., `atnog-io.iot4fire.av.it.pt:9092`) and the topic "rat-eos-pc"
          
### Important Notes on Adjusting Sensor Values

It is crucial to adjust the values in the `app.py` script according to the specific sensors being used. This includes ensuring the correct extraction of sensor measurements and maintaining the structured data model. 
Proper adherence to this data model is essential for the correct functioning of Kibana visualizations. 
