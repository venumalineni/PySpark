from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('group_by').getOrCreate()

data1 = [(1, 'Venu', 'M', 4400), (2, 'Surya', 'M', 5000),(3, 'Naga', 'M', 3000), (4, 'Sajid', 'M', 3500), (5, 'Sindhu', 'F', 3000), (6, 'Ratna', 'M', 3000)]
schema1 = ['id', 'name', 'gender', 'salary']

data2 = [(1, 'OSKAR'), (2, 'OSKAR'), (3, 'OSKAR'), (4, 'SRE'), (5, 'Sindhu'),(7, 'Naveen')]
schema2 = ['id', 'team']

empDF = spark.createDataFrame(data1, schema1)
teamDF = spark.createDataFrame(data2, schema2)

empDF.show()
teamDF.show()

empDF.join(teamDF, empDF.id == teamDF.id, 'leftsemi').show()
empDF.join(teamDF, empDF.id == teamDF.id, 'leftanti').show()

# self join

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('group_by').getOrCreate()

data3 = [(1, 'Venu', 2), (2, 'Surya', 3),(3, 'Sunila', 4)]
schema3 = ['id', 'name', 'managerId']

df = spark.createDataFrame(data3, schema3)
df.alias('empData').join(df.alias('mgrData'), col('empData.managerId') == col('mgrData.id'), 'left').\
    select(col('empData.name').alias('empName'), col('mgrData.name').alias('mgrName')).show()