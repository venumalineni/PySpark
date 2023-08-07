from pyspark.sql import SparkSession
from pyspark.sql.functions import json_tuple

spark = SparkSession.builder.appName('from_json').getOrCreate()

# converting from MapType() to to_json type
data = [('venu', '{"location":"US", "work":"COX Media Group"}'),
        ('sravani', '{"location":"IND", "work":"KPI Partners"}')]
schema = ['name', 'details']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

df1 = df.select(df.name, json_tuple(df.details, 'location', 'work').alias('country', 'company'))
df1.show()
