from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('show_function').getOrCreate()
data = [(1, 'Venu', 'Atlanta, working in COX Media Group'),
        (2, 'Sravani', 'Hyderabad, Working in KPI Partners'),
        (3, 'Priyal', 'Hyderabad, Studying in Kennedy Global School'),
        (4, 'Jaanvi', 'Hyderabad, Enjoying')]

schema = ['id', 'name', 'place']

df = spark.createDataFrame(data=data, schema=schema)
df.show()  # will show only first 20 characters
df.show(truncate=8) # if we want to truncate to 8 characters
df.show(truncate=False) # if we want to display all characters
df.show(n=2, truncate=False) # if we want to display only first 2 rows with all characters
df.show(n=4, truncate=False, vertical=True) # to display records in vertical format