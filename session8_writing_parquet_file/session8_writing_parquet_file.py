from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Python_Learning').getOrCreate()


data = ((1, 'Venu'), (2, 'Sravani'))
schema = ['id', 'name']

df = spark.createDataFrame(data=data, schema=schema)

df.write.parquet(r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\parquetdata', mode='ignore')
df.show()