from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, IntegerType
from pyspark.sql.functions import explode, split, array, array_contains, col

spark = SparkSession.builder.appName('arrary_additional_functions').getOrCreate()


# using explode() function
data = [(1, 'Venu', [35, 75, 5]), (2, 'Sravani', [29, 55, 4]), (3, 'Piryal', [6, 23, 3]), (4, 'Jaanvi', [2, 7, 2])]
schema = StructType([StructField('id', IntegerType()), StructField('name', StringType()), StructField('awh', ArrayType(IntegerType()))])
#schema = ['id', 'name', 'awh']

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

df1 = df.withColumn('explodedCol', explode(col('awh')))
df1.show()
df1.printSchema()

# using split() function
data1 = [(1, 'Venu', 'AWS,BO,Tableau,QlikView'), (2, 'Sravani', 'Mainframes,BO,PowerBI')]
schema1 = ['id', 'name', 'skills']

df3 = spark.createDataFrame(data1, schema1)
df3.show()
df3.printSchema()

df4 = df3.withColumn('skillsArrary', split(col('skills'),','))
df4.show()

# using array() function

data2 = [(1, 'Venu', 'Priyal', 'Jaanvi'), (2, 'Sravani', 'Priyal', 'Jaanvi')]
schema2 = ['id', 'name', 'ElderDaughter', 'YoungerDaughter']

df5 = spark.createDataFrame(data2, schema2)
df6 = df5.withColumn('kids', array(col('ElderDaughter'),col('YoungerDaughter')))

df6.show()
df6.printSchema()

# using array_contrain() function

data3 = [(1, 'Venu',['DE','DA']), (2,'Sravani',['DA','MF'])]
schema3 = ['id', 'name', 'skills']

df7 = spark.createDataFrame(data3,schema3)
df8 = df7.withColumn('Have DA Skill?', array_contains(col('skills'),'DA'))

df8.show()
df8.printSchema()