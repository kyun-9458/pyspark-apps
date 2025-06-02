from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName('HW') \
    .getOrCreate()

Conutryschema = 'ID INT, COUNTRY STRING, HIT LONG'
df = spark.createDataFrame(data=[(1, 'Korea', 120), (2, 'USA', 80), (3, 'Japan', 40)], schema = Conutryschema)

df.schema
df.count()
df.show()

# sleep 10m
time.sleep(600)