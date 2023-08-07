from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, months_between, add_months, date_add, year, month

spark = SparkSession.builder.appName('from_json').getOrCreate()

df = spark.createDataFrame([('2023-08-07', '2023-01-01')], ['d1', 'd2'])

df.withColumn('diff', datediff(df.d1, df.d2)).show()
df.withColumn('monthsBetween', months_between(df.d2, df.d1)).show()
df.withColumn('addmonth', add_months(df.d2,4)).show()
df.withColumn('submonth', add_months(df.d2,-4)).show()
df.withColumn('addDate', date_add(df.d2,4)).show()
df.withColumn('subDate', date_add(df.d2,-4)).show()
df.withColumn('year', year(df.d2)).show()
df.withColumn('month', month(df.d2)).show()