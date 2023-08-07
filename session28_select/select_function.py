from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('group_by').getOrCreate()

data = [(1, 'Venu', 'OSKAR', 'M', 4400), (2, 'Surya', 'OSKAR', 'M', 5000)]
schema = ['id', 'name', 'team', 'gender', 'salary']

df = spark.createDataFrame(data, schema)

df.select([col for col in df.columns]).show()
df.select('*').show()
df.select('id', 'name', 'team', 'gender', 'salary').show()
df.select(df.id, df.name, df.gender, df.salary).show()
df.select(df['id'], df['name'], df['salary']).show()

# using col() function
df.select(col('id'), col('name')).show()
df.select(['id', 'name']).show()