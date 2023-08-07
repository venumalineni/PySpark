from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('column_class').getOrCreate()

data = [('venu', 'male', 990000), ('sravani', 'female', 1000000)]
schema = ['name', 'gender', 'salary']

df = spark.createDataFrame(data, schema)

df.show()
df.printSchema()

# creating new column
df1 = df.withColumn('takehome_sale', lit(df.salary*.9))
df1.show()
df1.printSchema()

# Accessing the column in multiple ways from dataframe
df.select(df.gender).show()
df.select(df['gender']).show()
df.select(col('gender')).show()

data1 = [('Venu', 'Male', 990000, ('Priyal', 'Jaanvi')), ('Sravani', 'Female', 100000, ('Priyal', 'Jaanvi'))]
#schema1 = ['name', 'gender', 'salary', 'daughters']

daughtersType = StructType([StructField('ElderDaughter', StringType()), StructField('YoungerDaughter', StringType())])

schema1 = StructType([StructField('name',StringType()), StructField('gender', StringType()), StructField('salary', IntegerType()), StructField('daughters', daughtersType)])

df2 = spark.createDataFrame(data1, schema1)
df2.show()
df2.printSchema()

df2.select(df2.daughters.ElderDaughter).show()
df2.select(df2['daughters.YoungerDaughter']).show()
df2.select(col('daughters.YoungerDaughter')).show()