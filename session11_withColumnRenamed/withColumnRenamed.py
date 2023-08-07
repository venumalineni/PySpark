from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('with_column_rename').getOrCreate()

us_friends_data = [(1, 'Venu', '120000'), (2, 'Avinash', '200000'), (3, 'Balaram', '150000')]
columns = ['id', 'name', 'salary']

df = spark.createDataFrame(data=us_friends_data, schema=columns)

df.show()

df1 = df.withColumnRenamed('salary', 'salary_amount')

df1.show()