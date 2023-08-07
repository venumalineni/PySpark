from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('when_otherwise').getOrCreate()

data = [('venu', 'male', 35), ('sravani', 'female', 30), ('priyal', 'female', 6), ('jaanvi', 'female', 2)]
schema = ['name', 'gender', 'age']

data1 = [('venu', 'male', 50000), ('subbarao', 'male', 50000), ('veeramma', 'female', 100000)]
schema1 = ['name', 'gender', 'salary']

df1 = spark.createDataFrame(data, schema)
df2 = spark.createDataFrame(data1, schema1)

print('with normal union function the output is ')
df3 = df1.union(df2)
df3.show()

print('with unionByName function the output is ')
df4 = df1.unionByName(allowMissingColumns=True, other=df2)
df4.show()