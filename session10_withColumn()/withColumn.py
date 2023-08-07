from pyspark.sql.functions import col,lit
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('with_column').getOrCreate()

data = [(1, 'Venu', '100000'), (2, 'Sravani', '2200000')]
columns = ['id', 'name', 'salary']

df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)
df.printSchema()

df1= df.withColumn(colName='salary', col=col('salary').cast('Integer')) # to change the data type of a column

df1.show(truncate=False)
df1.printSchema()

df2 = df1.withColumn('salary', col('salary') * 2) # to do calculations on existing column
df2.show()
df2.printSchema()

df3 = df2.withColumn('country', lit('India'))  # to add new column country with lit function
df3.show()

df4 = df3.withColumn('next_year_salary', col('salary')* 0.8) # to create newly calculated column
df4.show()