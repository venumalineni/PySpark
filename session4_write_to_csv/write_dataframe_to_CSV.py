import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName('your_application_name').getOrCreate()

data = [(1,'Venu'), (2,'Sravani')]
schema = ['id', 'name']

df = spark.createDataFrame(data=data, schema=schema)

#df.write.json(r'C:\Users\vmalineni\Documents\jsondata/')
df.write.option('header=', True).csv(r'C:\Users\vmalineni\Documents\csv/', mode='overwrite')

df.show()