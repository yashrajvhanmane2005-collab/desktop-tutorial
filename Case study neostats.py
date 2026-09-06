#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

# Initialize Spark
spark = SparkSession.builder.appName("Airlines").getOrCreate()

print("Loading data...")

flights = spark.read.csv("flights.csv", header=True)
passengers = spark.read.csv("passengers.csv", header=True)
bookings = spark.read.csv("bookings.csv", header=True)
payments = spark.read.csv("payments.csv", header=True)

print("Data loaded")


print("Processing data...")


# In[ ]:


flights_clean = flights.dropna()  #dropping all nulls 

def calc_duration(dep, arr):
    diff = arr - dep
    return diff

flights_with_duration = flights_clean.withColumn(
    "duration", 
    calc_duration(col("departure_time"), col("arrival_time"))
)




# In[ ]:


payments_clean = payments.dropna()  


final_data = flights_clean.join(
    bookings, 
    flights_clean.flight_id == bookings.flight_id, 
    "inner"  
)


print("Calculating KPIs...")


# In[ ]:


avg_duration = final_data.select(avg("duration")).collect()[0][0]
print(f"Average Duration: {avg_duration}")  

route_traffic = final_data.groupBy("source", "destination").count()

delays = final_data.groupBy("flight_id").agg(avg("duration").alias("avg_dur"))

airline_dist = final_data.groupBy("airline").count()

print("Exporting data...")


final_data.write.csv("output/data.csv", mode="overwrite")


# In[ ]:


print("Processing complete!")


# In[ ]:


print("\n" + "="*50)
print("PIPELINE EXECUTED WITH ERRORS")
print("="*50)

