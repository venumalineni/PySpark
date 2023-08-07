from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import col, array

spark = SparkSession.builder.appName('array_type').getOrCreate()

data = [(1, 'Venu', [3, 4, 5]), (2, 'Naga', [1, 2, 3]), (3, 'Surya', [5, 6, 7])]
schema = StructType([StructField('id', IntegerType()), StructField('name', StringType()), StructField('bonus', ArrayType(IntegerType()))])

df = spark.createDataFrame(data=data, schema=schema)

df.show()
df.printSchema()

df1 = df.withColumn('2020 Bonus', df.bonus[0])

df1.show()
df1.printSchema()


data1 = [[1,2], [3,4]]
schema1 = ['num1', 'num2']

df2 = spark.createDataFrame(data1, schema1)
#df3 = df2.withColumn('numbers', array(col('num1'), array(col('num2'))))

df2.show()



