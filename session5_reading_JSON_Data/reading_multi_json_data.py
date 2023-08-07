import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName('your_application_name').getOrCreate()

# schema = StructType.add(field='id', data_type=IntegerType()) \
#                     .add(field='name', data_type=StringType()) \
#                     .add(field='gender', data_type=StringType()) \
#                     .add(field='salary', data_type=IntegerType())

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])
df = spark.read.json(path=[r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\Session5_jsonfiles\multiple_files\family1.json', r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\Session5_jsonfiles\multiple_files\family2.json'], schema=schema)
#df = spark.read.json(path=r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\multiple_files\*.json')

df.printSchema()
df.show()


