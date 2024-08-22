from cgi import test
from pickle import NONE
from sre_parse import CATEGORIES
from pandas.tseries.offsets import Second
import prophet
from prophet.plot import plot_components_plotly, plot_plotly

import wget
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import plotly.express as px
import dash
from dash import Input, Output, dcc, html
import plotly.graph_objs as go
import numpy as np
import datetime


import findspark
findspark.init() 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, avg
spark = SparkSession.builder.appName("NYCTaxiAnalysis").getOrCreate()
print(spark)
df = spark.read.csv('yellow_tripdata_2015-01.csv',header=True,inferSchema=True)
# Filter out rows with null values in critical columns
df = df.dropna(subset=["trip_distance", "fare_amount", "passenger_count"])
print(df)

# Filter based on realistic values
df = df.filter((df["trip_distance"] > 0) & (df["fare_amount"] > 0) & (df["passenger_count"] > 0))

df = spark.read.csv('yellow_tripdata_2015-01.csv', header=True, inferSchema=True)

# Show the schema of the dataset
df.printSchema()

# Data Cleaning
df = df.dropna(subset=["trip_distance", "fare_amount", "passenger_count"])

# Filter unrealistic values
df = df.filter((col("trip_distance") > 0) & (col("fare_amount") > 0) & (col("passenger_count") > 0))

# Data Transformation
# Calculate trip duration in minutes
df = df.withColumn("trip_duration", 
                   (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60)

# Calculate average speed (miles per hour)
df = df.withColumn("average_speed", 
                   col("trip_distance") / (col("trip_duration") / 60))

# Data Aggregation
# Average fare per hour
hourly_fare = df.groupBy(hour("tpep_pickup_datetime").alias("hour")).agg(avg("fare_amount").alias("avg_fare"))
hourly_fare.show()

# Busiest pickup locations
pickup_locations = df.groupBy("pickup_latitude", "pickup_longitude").count().orderBy("count", ascending=False)
pickup_locations.show()
hourly_fare.write.csv("hourly_fare_output.csv", header=True)
pickup_locations.write.csv("pickup_locations_output.csv", header=True)

spark.stop()



