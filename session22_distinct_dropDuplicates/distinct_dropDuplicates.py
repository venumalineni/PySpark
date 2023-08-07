from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('when_otherwise').getOrCreate()

data = [('venu', 'male', 35, 50000), ('sravani', 'female', 30, 100000), ('priyal', 'female', 6, 1000), ('jaanvi', 'female', 2, 2000),('venu', 'male', 35, 50000)]
schema= ['name', 'gender', 'age', 'salary']

df = spark.createDataFrame(data, schema)

df1 = df.distinct()
df1.show()

df2 = df.dropDuplicates()
df2.show()

df3 = df.dropDuplicates(['gender'])
df3.show()