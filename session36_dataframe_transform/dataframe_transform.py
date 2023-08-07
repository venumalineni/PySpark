from pyspark.sql import SparkSession
from pyspark.sql.functions import lower

spark = SparkSession.builder.appName('dataframe_transform').getOrCreate()

data = [(1, 'Venu', 'OSKAR', 'M', 4400), (2, 'Surya', 'OSKAR', 'M', 5000), (3, 'Naga', 'OSKAR', 'M', 3000), (4, 'Sajid', 'SRE', 'M', 3500), (5, 'Sindhu', 'SRE', 'F', 3000),
        (6, 'Ratna', 'NCMR', 'M', 3500), (7, 'Naveen', 'NCMR', 'M', 4000), (8, 'Krishna', 'FICC', 'M', 5000), (9, 'Pallavi', 'FICC', 'F', 3000)]
schema = ['id', 'name', 'team', 'gender', 'salary']

df = spark.createDataFrame(data, schema)
df.show()

def convertToLower(df):
    return df.withColumn('Team', lower(df.team))

def doubleTheSalary(df):
    return df.withColumn('doubleSalary', df.salary * 2)

df1 = df.transform(convertToLower).transform(doubleTheSalary)
df1.show()



