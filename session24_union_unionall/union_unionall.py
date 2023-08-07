from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('when_otherwise').getOrCreate()

data = [('venu', 'male', 35, 50000), ('sravani', 'female', 30, 100000), ('priyal', 'female', 6, 1000), ('jaanvi', 'female', 2, 2000)]
schema= ['name', 'gender', 'age', 'salary']

data1 = [('venu', 'male', 35, 50000), ('subbarao', 'male', 70, 50000), ('veeramma', 'female', 30, 100000)]
schema1 = ['name', 'gender', 'age', 'salary']

df1 = spark.createDataFrame(data, schema)
df1.show()

df2 = spark.createDataFrame(data1, schema1)
df2.show()

df3 = df1.unionAll(df2)

df5 = df4.distinct()
df5.show()