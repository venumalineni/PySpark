from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName('your_application_name').getOrCreate()

df = spark.read.parquet(r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\session6_parquetfiles\userdata1.parquet')
#df = spark.read.parquet([r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\session6_parquetfiles\userdata1.parquet',r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\session6_parquetfiles\userdata2.parquet'])


df.show()
print('count of dataframe is ' + str(df.count()))