from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json
from pyspark.sql.types import MapType, StringType, StructType, StructField

spark = SparkSession.builder.appName('from_json').getOrCreate()

# converting from MapType() to to_json type
data = [('venu', {'countryOfBirth':'India', 'Studies':'United Kingdom'})]
schema = ['name', 'country_visited']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()


df1 = df.withColumn('countries', to_json(df.country_visited))
df1.show(truncate=False)
df1.printSchema()

# converting from StructType() to to_json type
data1 = [('venu', ('Priyal', 'Jaanvi'))]
schema1 = StructType([StructField('name', StringType()),
                      StructField('Daughters', StructType([StructField('ElderDaughter', StringType()), StructField('YoungerDaughter', StringType())]))])

df = spark.createDataFrame(data1, schema1)
df.show(truncate=False)
df.printSchema()


df1 = df.withColumn('DaughterNames', to_json(df.Daughters))
df1.show(truncate=False)
df1.printSchema()
