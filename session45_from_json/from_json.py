from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType, StructType, StructField

spark = SparkSession.builder.appName('from_json').getOrCreate()

data = [('venu', '{"Native": "India", "studies":"United Kingdom", "tour":"Malaysia", "work":"United States"}')]
schema = ['name', 'country_visited']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

StructSchema = StructType([StructField('Native', StringType()), StructField('studies', StringType()),\
                          StructField('tour', StringType()), StructField('work', StringType())])

df1 = df.withColumn('countries', from_json(df.country_visited, StructSchema))
df1.show(truncate=False)
df1.printSchema()

df2 = df1.withColumn('work_country', df1.countries.work)
df2.show(truncate=False)
df2.printSchema()