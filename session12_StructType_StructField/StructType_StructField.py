from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('structtype_struckfield').getOrCreate()

data = [(1, ('Venu','Malineni'), 100000), (2, ('Sravani','Madala'), 200000)]

structName = StructType([StructField('firstname', StringType()),
                        StructField('lastname', StringType())])

schema = StructType([StructField(name='id', dataType=IntegerType()),
                     StructField(name='name', dataType=structName),
                     StructField(name='salary', dataType=IntegerType())])


df = spark.createDataFrame(data=data, schema=schema)

df.show()

df.printSchema()