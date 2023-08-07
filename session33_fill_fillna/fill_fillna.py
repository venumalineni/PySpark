from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('fill_fillna').getOrCreate()

data = [(1, 'Venu', 'OSKAR', 'M', 4400), (2, 'Surya', 'OSKAR', 'M', 5000), (3, 'Naga', 'OSKAR', 'M', 3000), (4, 'Sajid', 'SRE', 'M', 3500), (5, 'Sindhu', None, 'F', None),
        (6, 'Ratna', 'NCMR', 'M', 3500), (7, 'Naveen', 'NCMR', 'M', None), (8, 'Krishna', 'FICC', 'M', 5000), (9, 'Pallavi', None, 'F', None)]
schema = ['id', 'name', 'team', 'gender', 'salary']

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

df1 = df.na.fill('unknown', ['team', 'salary'])
df1.show()

df2 = df.fillna('Bench', 'team')
df3 = df2.fillna(1000, 'salary')
df3.show()
