from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName('Test_pyspark') \
    .getOrCreate()

schema = 'NAME STRING, AGE INT, GF BOOLEAN'
fr = spark.createDataFrame(data=[('KK', 29, True), ('JS', 29, False), ('MJ', 30, False), ('JM', 29, False), ('TwoWeeks Dragon', 29, True)], schema = schema)
fr.show()

schemaa = 'NAME STRING, AGE INT, MARRIAGE BOOLEAN'
df = spark.createDataFrame(data=[('hong',28, False),('kim',33,True)], schema=schemaa)
df.show()

time.sleep(60)
