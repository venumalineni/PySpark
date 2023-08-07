from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('group_by').getOrCreate()

data = [(1, 'Venu', 'OSKAR', 'M', 4400), (2, 'Surya', 'OSKAR', 'M', 5000), (3, 'Naga', 'OSKAR', 'M', 3000), (4, 'Sajid', 'SRE', 'M', 3500), (5, 'Sindhu', 'SRE', 'F', 3000),
        (6, 'Ratna', 'NCMR', 'M', 3500), (7, 'Naveen', 'NCMR', 'M', 4000), (8, 'Krishna', 'FICC', 'M', 5000), (9, 'Pallavi', 'FICC', 'F', 3000)]
schema = ['id', 'name', 'team', 'gender', 'salary']

df = spark.createDataFrame(data, schema)

df1 = df.groupBy('team').count()
df1.show()

df2 = df.groupBy('team', 'gender').sum('salary')
df2.show()

df3 = df.groupBy('team', 'gender').avg('salary')
df3.show()

df4 = df.groupBy('team', 'gender').max('salary')
df4.show()