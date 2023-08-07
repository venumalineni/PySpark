from pyspark.sql import SparkSession
from pyspark.sql.functions import transform, upper, lower

spark = SparkSession.builder.appName('dataframe_transform').getOrCreate()

data = [(1, 'Venu', ['BO', 'Informatica', 'Qlikview', 'Tableau', 'AWS']), (2, 'Surya', ['SQL', 'Management', 'DBA']),
        (3, 'Naga', ['AWS', 'Redshift'])]
schema = ['id', 'name', 'skills']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)

df1 = df.select('id', 'name', transform('skills', lambda x: upper(x)).alias('Technologies'))
df1.show(truncate=False)


def converArraytoLower(x):
    return lower(x)


df2 = df1.select('id', 'name', transform('Technologies', converArraytoLower)).alias('TechListInSmallCase')
df2.show(truncate=False)