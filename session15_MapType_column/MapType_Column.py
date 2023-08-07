from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType

spark = SparkSession.builder.appName('maptype_column').getOrCreate()

data = [('venu', {'quailification':'Masters','job':'data engieer','location':'US'}),('sravani', {'quailification':'Btech','job':'data analytics','location':'India'})]
#schema = ['name', 'professionalDetails']
schema = StructType([StructField('name', StringType()), StructField('professionalDetails',MapType(StringType(),StringType()))])


df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()

df1 = df.withColumn('Qualification', df.professionalDetails['quailification'])
df1.show(truncate=False)

df2 = df1.withColumn('Job', df1.professionalDetails['job'])
df2.show(truncate=False)

df3 = df2.withColumn('location', df2.professionalDetails.getItem('location'))
df3.show(truncate=False)
df3.printSchema()