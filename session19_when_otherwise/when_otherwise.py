from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName('when_otherwise').getOrCreate()

data = [('venu','male',35),('sravani','female',29),('priyal','female',6),('jaanvi','female',2)]
schema= ['name','gender','age']

df = spark.createDataFrame(data,schema)

df.show()
df.printSchema()

df1 =df.select(df.name, when(condition=df.gender=='male',value='M').when(condition=df.gender=='female',value='F').otherwise('unknown').alias('gender_short'), df.age)

df1.show()
df1.printSchema()