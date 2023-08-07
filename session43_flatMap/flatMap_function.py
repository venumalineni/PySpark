from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('flat_map').getOrCreate()

data = ['Venu Malineni', 'Sravani Madala']

rdd = spark.sparkContext.parallelize(data)

for item in rdd.collect():
    print(item)

rdd1 = rdd.flatMap(lambda x: x.split(' '))

for item in rdd1.collect():
    print(item)

