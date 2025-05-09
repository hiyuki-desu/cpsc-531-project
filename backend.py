from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
import json
from openaq import OpenAQ
import schema

"""
Location ID
Anaheim: 8874, 8875

8875 = sensor ID:
    25902, 4272338, 25901, 4272072, 25900, 25899, 25898

8874 sensor ID:
    25897, 4272174, 25896, 4272267    

send

"""
#Modify data here
dateTimeFrom = '2025-04-17'
dateTimeTo = '2025-05-07'

# Initialize Spark Session
spark = SparkSession.builder.appName("Project OpenAQ").getOrCreate()

# Define API URL with API key
API_KEY = "5f499b458366f22051e7b12d10da514ea244d442fa1e18206a41944d957e59ed" #i know it's bad practice

client = OpenAQ(api_key=API_KEY) #connect to OpenAQ to collect data

#Helper methods for fetching data from OpenAQ API
def fetch_data_from_api(url, location_code):
    response = url.locations.get(location_code) #location data
    if hasattr(response, 'status_code'):
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")
    else:
        return json.loads(response.json()) # I don't know why response.json() returns a str type here???? 
                                           # So I have to wrap with another JSON

#Fetching measurements
#Can be changed to get latest measurement
def fetch_measurement_from_api(url, sensor_id):
    #get measurement from range
    response = url.measurements.list(sensor_id, data='measurements', rollup='daily', datetime_from=dateTimeFrom, 
                                    datetime_to=dateTimeTo, page=1, limit=1000)
    if hasattr(response, 'status_code'):
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")
    else:
        return json.loads(response.json()) #Same as Line 38

#Fetch data
locations = [8874, 8875]
sensor_ids = [[25902, 4272338, 25901, 4272072, 25900, 25899, 25898], 
              [25897, 4272174, 25896, 4272267]]
input_path = []
raw_data_list = []
for location_code in locations:
    try:
        raw_data = fetch_data_from_api(client, location_code)
        raw_data_list.append(raw_data)
        with open(str(location_code), 'w') as outfile:
            json.dump(raw_data, outfile)
            input_path.append(str(location_code)+'.json')


    except Exception as e:
        print(e)
        raw_data = []

measure_raw_data_list = []
for sensor_group in sensor_ids:
    for sensor in sensor_group:
        try:
            measure_raw_data = fetch_measurement_from_api(client, sensor)
            measure_raw_data_list.append(measure_raw_data)
            with open(str(sensor), 'w') as outfile:
                json.dump(measure_raw_data, outfile)
                input_path.append(str(sensor)+'.json')
        except Exception as e:
            print(e)
            measure_raw_data = []


print(input_path)
#Convert to Spark DataFrame
#create empty rdd
empty_RDD = spark.sparkContext.emptyRDD()
location_df = spark.createDataFrame(data=empty_RDD, schema=schema.location_schema)
if raw_data:
    for rawData in raw_data_list:
        data_list = rawData.get("results", [])
        df = spark.createDataFrame(data_list, schema=schema.location_schema)
        #df.show()
        location_df = location_df.union(df)
else:
    print("No data retrieved from API.")

sensor_id_flat = []
for sublist in sensor_ids:
    for item in sublist:
        sensor_id_flat.append(item) 

measurement_RDD_list = []
if measure_raw_data:
    for i in range(len(measure_raw_data_list)):
        m_list = measure_raw_data_list[i].get("results", [])
        measurement_RDD = spark.createDataFrame(m_list, schema=schema.measurement_schema)
        measurement_RDD2 = measurement_RDD.withColumn("sensor_id", lit(sensor_id_flat[i]))
        #measurement_RDD.show()
        measurement_RDD_list.append(measurement_RDD2)

else:
    print("No measurement retried from API.")




#combine all measurement into 1 dataframe
measurement_df = reduce(DataFrame.unionAll, measurement_RDD_list)

#output into json file
location_df.write.json("./location_dataframe")
measurement_df.write.json("./measurement_dataframe")

# Stop Spark session
client.close()
spark.stop()
