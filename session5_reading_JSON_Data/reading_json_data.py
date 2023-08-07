import spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('your_application_name').getOrCreate()

df = spark.read.json(path=r'C:\Users\vmalineni\Documents\mywork\PySpark_Learning\Session5_jsonfiles\multiple\family_Multi.json', multiLine=True)

df.printSchema()
df.show()