#!/usr/bin/env python
# coding: utf-8

# Importing Apache Spark modules/packages
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create & Initialize Session
spark = SparkSession.builder.master("local[*]").getOrCreate()

### LANDING LAYER - GROUP_BOOKING_REQUEST TABLE 

# Create dataframe

# a. Define the schema
df_schema = StructType([
StructField("id_request", IntegerType(),True),
StructField("pos", StringType(),True),
StructField("office_type", StringType(),True),
StructField("airport_origin", StringType(),True),
StructField("airport_destination", StringType(),True),
StructField("flight_date", DateType(),True),
StructField("seats", IntegerType(),True),
StructField("booking", StringType(),True),
StructField("request_status", StringType(),True),
StructField("request_date", TimestampType(),True),
StructField("sent_date", TimestampType(),True),
])

# b. Define file path
gcs_file_path = "gs://it-analytics-inventory-380100-dev/datalake/workload/group_booking_request/group_booking_request.csv"

# c. Load data
df_group_booking_request = spark.read.format("CSV").option("header","true").option("delimiter",",").schema(df_schema).load(gcs_file_path)

# Save dataframe to a Landing layer path
gcs_destination_path = "gs://it-analytics-inventory-380100-dev/datalake/landing/group_booking_request/"
df_group_booking_request.write.mode("overwrite").format("parquet").save(gcs_destination_path)

### LANDING LAYER - GROUP_BOOKING_REQUEST_MOVEMENT TABLE

# Create dataframe

# a. Define the schema
df_schema = StructType([
StructField("id_request_movement", IntegerType(),True),
StructField("id_request", IntegerType(),True),
StructField("movement_date", TimestampType(),True),
StructField("movement", StringType(),True),
StructField("comments", StringType(),True),
StructField("movement_error_flag", BooleanType(),True),
])

# b. Define file path
gcs_file_path = "gs://it-analytics-inventory-380100-dev/datalake/workload/group_booking_request_movement/group_booking_request_movement.csv"

# c. Load data
df_group_booking_request_movement = spark.read.format("CSV").option("header","true").option("delimiter",",").schema(df_schema).load(gcs_file_path)


# Save dataframe to a Landing layer path
gcs_destination_path = "gs://it-analytics-inventory-380100-dev/datalake/landing/group_booking_request_movement/"
df_group_booking_request_movement.write.mode("overwrite").format("parquet").save(gcs_destination_path)


### LANDING LAYER - AIRPORT TABLE

# Create dataframe
# a. Define the schema
df_schema = StructType([
StructField("IATA", StringType(),True),
StructField("ICAO", StringType(),True),
StructField("NAME", StringType(),True),
StructField("COUNTRY", StringType(),True),
StructField("ELEVATION", StringType(),True),
StructField("LATITUDE", StringType(),True),
StructField("LONGITUDE", StringType(),True),
StructField("WIKIPEDIA", StringType(),True),
])

# b. Define file path
gcs_file_path = "gs://it-analytics-inventory-380100-dev/datalake/workload/external_sources/airports.csv"

# c. Load data
df_airport = spark.read.format("CSV").option("header","true").option("delimiter",",").schema(df_schema).load(gcs_file_path)

# Save dataframe to a Landing layer path
gcs_destination_path = "gs://it-analytics-inventory-380100-dev/datalake/landing/external_sources/"
df_airport.write.mode("overwrite").format("parquet").save(gcs_destination_path)


### CURATED LAYER - GROUP_BOOKING_REQUEST TABLE

# Define file path
gcs_landing_path = "gs://it-analytics-inventory-380100-dev/datalake/landing/group_booking_request/"

# Create GROUP_BOOKING_REQUEST Dataframe
df_group_booking_request_landing = spark.read.format("parquet").option("header","true").load(gcs_landing_path)

# Validation
# a. Filter out rows where id_request & request_date is not null
filtered_df_validation_1 = df_group_booking_request_landing.filter(col("id_request").isNotNull() & col("request_date").isNotNull())

# b. Filter out rows where length(pos) <= 6
filtered_df_validation_2 = filtered_df_validation_1.filter((length(col("pos")) <= 6))

# c. Filter out rows where seats >= 0
filtered_df_validation_3 = filtered_df_validation_2.filter(col("seats") >= 0)

# Define destination path in curated layer
gcs_curated_group_booking_request_path = "gs://it-analytics-inventory-380100-dev/datalake/curated/group_booking_request/"

# Load data
filtered_df_validation_3.repartition(1).write.mode("overwrite").format("parquet").save(gcs_curated_group_booking_request_path)


### CURATED LAYER - GROUP_BOOKING_REQUEST_MOVEMENT TABLE

# Define file path
gcs_landing_path = "gs://it-analytics-inventory-380100-dev/datalake/landing/group_booking_request_movement/"

# Create GROUP_BOOKING_REQUEST_MOVEMENT Dataframe
df_group_booking_request_movement_landing = spark.read.format("parquet").option("header","true").load(gcs_landing_path)

# Validation
# a. Filter out rows where id_request, id_request_movement, movement_date are not null
filtered_df_validation = df_group_booking_request_movement_landing.filter(col("id_request").isNotNull() & col("id_request_movement").isNotNull() & col("movement_date").isNotNull())

# Define destination path in curated layer
gcs_curated_group_booking_request_movement_path = "gs://it-analytics-inventory-380100-dev/datalake/curated/group_booking_request_movement/"

# Load data
filtered_df_validation.repartition(1).write.mode("overwrite").format("parquet").save(gcs_curated_group_booking_request_movement_path)



### CURATED LAYER - AIRPORT TABLE

# Define file path
gcs_landing_path = "gs://it-analytics-inventory-380100-dev/datalake/landing/external_sources/"

# Create AIRPORT Dataframe
df_airport_landing = spark.read.format("parquet").option("header","true").load(gcs_landing_path)

# Validation
# a. Filter out rows where IATA, ICAO, NAME AND COUNTRY is not null
filtered_df_validation_1 = df_airport_landing.filter(col("IATA").isNotNull() & col("ICAO").isNotNull() & col("NAME").isNotNull() & col("COUNTRY").isNotNull())

# Define destination path in curated layer
gcs_curated_airport_path = "gs://it-analytics-inventory-380100-dev/datalake/curated/external_sources"

# Load data
filtered_df_validation_1.repartition(1).write.mode("overwrite").format("parquet").save(gcs_curated_airport_path)



### FUNCTIONAL LAYER
# Define file path of curated tables
gcs_curated_group_booking_request = "gs://it-analytics-inventory-380100-dev/datalake/curated/group_booking_request/"
gcs_curated_group_booking_request_movement = "gs://it-analytics-inventory-380100-dev/datalake/curated/group_booking_request_movement/"
gcs_curated_airport = "gs://it-analytics-inventory-380100-dev/datalake/curated/external_sources/"

# Create dataframes
df_group_booking_request_curated = spark.read.format("parquet").option("header","true").load(gcs_curated_group_booking_request)
df_group_booking_request_movement_curated = spark.read.format("parquet").option("header","true").load(gcs_curated_group_booking_request_movement)
df_airport_curated = spark.read.format("parquet").option("header","true").load(gcs_curated_airport)

# Rename id_request to id_request2 inside df_group_booking_request_movement_curated to avoid ambiguity
df_group_booking_request_movement_curated = df_group_booking_request_movement_curated.withColumnRenamed("id_request", "id_request2")

# Join tables group_booking_request & group_booking_request_movement
df_group_booking_join = df_group_booking_request_curated.join(df_group_booking_request_movement_curated, df_group_booking_request_curated.id_request == df_group_booking_request_movement_curated.id_request2)

df_group_booking_join = df_group_booking_join.drop("id_request2")

# Join tables group_booking_join & airport for getting airport description
df_airport_origin = df_airport_curated.select("IATA", "NAME").withColumnRenamed("IATA", "iata_code_origin").withColumnRenamed("NAME", "airport_name_origin")
df_airport_destination = df_airport_curated.select("IATA", "NAME").withColumnRenamed("IATA", "iata_code_destination").withColumnRenamed("NAME", "airport_name_destination")

df_group_booking_airport_join = df_group_booking_join.join(df_airport_origin, df_group_booking_join.airport_origin == df_airport_origin.iata_code_origin)
df_group_booking_airport_join = df_group_booking_airport_join.join(df_airport_destination, df_group_booking_airport_join.airport_destination == df_airport_destination.iata_code_destination)

# Select only fields needed for the business case
df_group_booking_select = df_group_booking_airport_join.select(
    col('id_request'),
    col('office_type'),
    col('iata_code_origin'),
    col('iata_code_destination'),
    col('airport_name_origin'),
    col('airport_name_destination'),
    col('seats'),
    col('booking'),
    col('request_status'),
    col('request_date'),
    col('id_request_movement'),
    col('movement_date'),
    col('movement'),
    col('comments'),
    col('movement_error_flag'),

)

# Define path to save files into functional layer
path_group_booking_functional =  "gs://it-analytics-inventory-380100-dev/datalake/functional/group_booking/"

# Load data
df_group_booking_select.repartition(1).write.mode("overwrite").format("parquet").save(path_group_booking_functional)


