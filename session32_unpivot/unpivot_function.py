from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName('group_by').getOrCreate()

data = [('OSKAR', 3, 3), ('SRE', 2, 1), ('COLT', 5, 3)]
schema = ['team', 'male', 'female']

df = spark.createDataFrame(data, schema)
df.show()

unpivotDF = df.select('team', expr("stack(2, 'Male', male, 'Female', female) as (gender, count)"))
unpivotDF.show()
