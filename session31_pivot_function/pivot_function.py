from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('group_by').getOrCreate()

data = [(1, 'Venu', 'OSKAR', 'M', 4400), (2, 'Surya', 'OSKAR', 'M', 5000), (3, 'Naga', 'OSKAR', 'M', 3000), (4, 'Sajid', 'SRE', 'M', 3500), (5, 'Sindhu', 'SRE', 'F', 3000),
        (6, 'Ratna', 'NCMR', 'M', 3500), (7, 'Naveen', 'NCMR', 'M', 4000), (8, 'Krishna', 'FICC', 'M', 5000), (9, 'Pallavi', 'FICC', 'F', 3000)]

schema = ['id', 'name', 'team', 'gender', 'salary']

df = spark.createDataFrame(data, schema)
df.show()

df1 = df.groupBy('team', 'gender').count()
df1.show()

# if we want to pivot gender wise
df2 = df.groupBy('team').pivot('gender').count()
df2.show()

# if we want to pivot only male columns
df3 = df.groupBy('team').pivot('gender', ['M']).count()
df3.show()

# if we want to pivot team wise
df4 = df.groupBy('gender').pivot('team').count()
df4.show()

# if you want to pivot only particular teams
df5 = df.groupBy('gender').pivot('team', ['OSKAR', 'SRE']).count()
df4.show()