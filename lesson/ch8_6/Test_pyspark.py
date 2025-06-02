from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName('TEST_JKK') \
    .getOrCreate()

schema = 'NAME STRING, REGION STRING, AGE INT, GF BOOLEAN'
fr = spark.createDataFrame(data=[('KK', 29, True), ('JS', 29, False), ('MJ', 30, False), ('JM', 29, False), ('TwoWeeks Dragon', 29, True)], schema = schema)
fr.show()

schema2 = 'NAME STRING, AGE INT, MARRIAGE BOOLEAN'
df = spark.createDataFrame(data=[('hong',28, False),('kim',33,True)], schema=schema2)
df.show()

time.sleep(60)
