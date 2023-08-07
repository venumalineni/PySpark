from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('when_otherwise').getOrCreate()

data = [('venu', 'male', 35, 50000), ('sravani', 'female', 30, 100000), ('priyal', 'female', 6, 1000), ('jaanvi', 'female', 2, 2000),('venu', 'male', 35, 50000)]
schema= ['name', 'gender', 'age', 'salary']

df = spark.createDataFrame(data, schema)

# using sort function
df1 = df.sort('age', 'salary')
df1.show()

# using order by with desc, asc function
df2 = df.orderBy(df.name.desc(), df.salary.asc())
df2.show()

# using order by
df3 = df.orderBy('age', 'salary')
df3.show()