from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('range_function').getOrCreate()

df = spark.range(start=1, end=10)
df.show()

df1 = df.sample(fraction=0.1, seed=123)
df2 = df.sample(fraction=0.1, seed=123)
df1.show()
df2.show()