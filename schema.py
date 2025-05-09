from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, IntegerType, ArrayType, TimestampType

# Define the schema for Coordinates
coordinates_schema = StructType([
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
])

# Define the schema for CountryBase
country_base_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StringType(), True)
])

# Define the schema for OwnerBase
owner_base_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Define the schema for ProviderBase
provider_base_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Define the schema for InstrumentBase
instrument_base_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Define the schema for ParameterBase
parameter_base_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("units", StringType(), True),
    StructField("display_name", StringType(), True)
])

# Define the schema for SensorBase
sensor_base_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("parameter", parameter_base_schema, True)
])

# Define the schema for Datetime
datetime_schema = StructType([
    StructField("utc", TimestampType(), True),
    StructField("local", TimestampType(), True)
])

# Define the main schema for Location
location_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("country", country_base_schema, True),
    StructField("owner", owner_base_schema, True),
    StructField("provider", provider_base_schema, True),
    StructField("is_mobile", BooleanType(), True),
    StructField("is_monitor", BooleanType(), True),
    StructField("instruments", ArrayType(instrument_base_schema), True),
    StructField("sensors", ArrayType(sensor_base_schema), True),
    StructField("coordinates", coordinates_schema, True),
    StructField("bounds", ArrayType(FloatType()), True),
    StructField("distance", FloatType(), True),
    StructField("datetime_first", datetime_schema, True),
    StructField("datetime_last", datetime_schema, True)
])

period_schema = StructType([
    StructField("label", StringType(), True),
    StructField("interval", StringType(), True),
    StructField("datetime_from", datetime_schema, True),
    StructField("datetime_to", datetime_schema, True)
])

# Define the schema for Summary
summary_schema = StructType([
    StructField("min", FloatType(), True),
    StructField("q02", FloatType(), True),
    StructField("q25", FloatType(), True),
    StructField("median", FloatType(), True),
    StructField("q75", FloatType(), True),
    StructField("q98", FloatType(), True),
    StructField("max", FloatType(), True),
    StructField("avg", FloatType(), True),
    StructField("sd", FloatType(), True)
])


#Define the main schema for Measurement
measurement_schema = StructType([
    StructField("period", period_schema, True),
    StructField("value", FloatType(), True),
    StructField("parameter", parameter_base_schema, True),
    StructField("coordinates", coordinates_schema, True),
    StructField("coverage", StringType(), True),
    StructField("summary", summary_schema, True)
])
