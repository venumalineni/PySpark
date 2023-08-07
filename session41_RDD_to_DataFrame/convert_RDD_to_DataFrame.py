from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('group_by').getOrCreate()

data = [(1, 'Venu'), (2, 'Sravani')]
rdd = spark.sparkContext.parallelize(data)
print(type(data))
print(type(rdd))
print(rdd.collect())

df = rdd.toDF(schema=['id', 'name'])
df.show()

df1 = spark.createDataFrame(rdd, schema=['id', 'name'])
df1.show()