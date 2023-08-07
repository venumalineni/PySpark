from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format, lit, to_date

spark = SparkSession.builder.appName('from_json').getOrCreate()

df = spark.range(1)

df1 = df.withColumn('todaysDate', current_date())
df1.show()
df1.printSchema()

df2 = df.withColumn('newFormat', date_format(lit('2023-12-25'), 'MM.dd.yyyy'))
df2.show()
df2.printSchema()

df3 = df.withColumn('newDateCol', to_date(lit('2023-08-07'), 'dd-MM-yyyy'))
df3.show()
df3.printSchema()

df4 = df1.withColumn('currentDate', date_format(df1.todaysDate, 'dd.MM.yyyy'))
df4.show()
df4.printSchema()