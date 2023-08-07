from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('group_by').getOrCreate()

data = [(1, 'Venu', 'OSKAR', 'M', 4400, 200), (2, 'Surya', 'OSKAR', 'M', 5000, 300),
        (3, 'Naga', 'OSKAR', 'M', 3000, 100),
        (4, 'Sajid', 'SRE', 'M', 3500, 400), (5, 'Ratna', 'NCMR', 'M', 3500)]
schema = ['id', 'name', 'team', 'gender', 'salary', 'bonus']

df = spark.createDataFrame(data, schema)

# Creating function using udf with annotation syntax
@udf(returnType=IntegerType())
def totalPay(s, b):
    return s + b


#TotalPayment = udf(lambda s, b: totalPay(s, b), IntegerType())

df1 = df.select('*', totalPay(df.salary, df.bonus).alias('TotalPay'))
df1.show()

# to register the function in spark

spark.udf.register(name='totalPay', f=totalPay, returnType=IntegerType())