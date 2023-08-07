from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object

spark = SparkSession.builder.appName('from_json').getOrCreate()

# converting from MapType() to to_json type
data = [('venu', '{"address":{"city":"Atlanta", "state":"Georgia"}, "gender":"male"}'),
        ('sravani', '{"address":{"city":"Hyderabad", "state":"Telangana"}, "gender":"female"}')]
schema = ['name', 'details']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

df1 = df.select('name', get_json_object('details', '$.address.city').alias('city'))
df1.show(truncate=False)
df1.printSchema()