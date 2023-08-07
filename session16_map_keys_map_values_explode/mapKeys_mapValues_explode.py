from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, map_keys, map_values
from pyspark.sql.types import StructType, StructField, StringType, MapType

spark = SparkSession.builder.appName('mapKeys_mapValues_explode').getOrCreate()

data = [('venu', {'quailification':'Masters','job':'data engieer','location':'US'}),('sravani', {'quailification':'Btech','job':'data analytics','location':'India'})]
schema = StructType([StructField('name', StringType()), StructField('professionalDetails',MapType(StringType(),StringType()))])

# using explode() Function
df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

df1 = df.select('name', 'professionalDetails', explode(df.professionalDetails))

df1.show(truncate=False)
df1.printSchema()

# using map_keys() function

df2 = df.withColumn('keys', map_keys(df.professionalDetails))
df2.show(truncate=False)

# using map_values() function

df3 = df.withColumn('values', map_values(df.professionalDetails))

df3.show()
df3.printSchema()