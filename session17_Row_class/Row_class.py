from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName('row_class').getOrCreate()

# creating rows into dataframes using Row() class

row1 = Row(name = 'Venu', age = 35)
row2 = Row(name = 'Sravani', age = 30)

data = [row1, row2]

df = spark.createDataFrame(data)
df.show()
df.printSchema()

Person = Row('name','salary')
person1 = Person('Venu', 990000)
person2 = Person('Sravani', 100000)

personDF = spark.createDataFrame([person1, person2])
personDF.show()

# creating nested struct type also using Row()

nest_data = [Row(name="venu", details=Row(location='US',state='GA')), Row(name='Sravani', details=Row(location='IND',state='TN'))]

nestedDF = spark.createDataFrame(nest_data)
nestedDF.show()
nestedDF.printSchema()