from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, hour, minute, second

spark = SparkSession.builder.appName('from_json').getOrCreate()

df = spark.range(1)
df.show()

df1 = df.withColumn('timestamp', current_timestamp())
df.show(truncate=False)

df1.printSchema()
df2 = df1.withColumn('toTimestamp', to_timestamp(lit('25.12.2022 06.10.13'), 'dd.MM.yyyy HH.mm.ss'))
df2.show(truncate=False)

df2.select('id', hour(current_timestamp()).alias('hour'), minute(current_timestamp()).alias('min'), second(current_timestamp()).alias('second')).show()