from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('map_function').getOrCreate()

data = [('venu', 'malineni'), ('sravani', 'madala')]

rdd1 = spark.sparkContext.parallelize(data)
rdd2 = rdd1.map(lambda x: x + (x[0] + ' ' + x[1],))

print(rdd1.collect())

df = rdd2.toDF(['first name', 'last name', 'full name'])
df.show()


df1 = spark.createDataFrame(data, ['firstname', 'lastname'])
df1.show()

rdd3 = df.rdd.map(lambda x: x + (x[0], ' ' + x[1],))
df2 = rdd3.toDF(['firstname', 'lastname', 'fullname'])
df2.show()


def fullname(x):
    return x + (x[0]+' '+x[1],)

data1 = [('priyal', 'malineni'), ('jaanvi', 'malineni')]

df3 = spark.createDataFrame(data1, ['fn', 'ln'])
rdd4 = df3.rdd.map(lambda x: fullname(x))
df4 = rdd4.toDF(['fn', 'ln', 'fullname'])
df4.show()