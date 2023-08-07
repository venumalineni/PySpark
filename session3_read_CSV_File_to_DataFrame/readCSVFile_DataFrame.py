from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName('your_application_name').getOrCreate()

df = spark.read.csv(r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\session5\emp.csv')


df.show()
print('count of dataframe is ' + str(df.count()))