import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName('your_application_name').getOrCreate()

data = [(1,'Venu'), (2,'Sravani')]
schema = ['id', 'name']

df = spark.createDataFrame(data=data, schema=schema)

#df.write.json(r'C:\Users\vmalineni\Documents\jsondata/')
df.write.mode('overwrite').json(r'C:\Users\vmalineni\Documents\jsondata/', mode='overwrite')


df.show()