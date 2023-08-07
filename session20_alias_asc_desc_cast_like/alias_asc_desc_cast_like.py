from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('when_otherwise').getOrCreate()

data = [('venu','male',35,900),('sravani','female',30,1000),('priyal','female',6,200),('jaanvi','female',2,100)]
schema= ['name','gender','age','salary']

df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# Creating alias() for a column
df1 = df.select(df.name.alias('Family Member Name'),df.gender,df.age, df.salary)
df1.show()

# Sort by ascending and descending

df2 = df.sort(df.salary.desc())
df2.show()

df3 = df.sort(df.age.asc())
df3.show()

# Cast to convert the data type

df4 = df.select(df.name, df.gender, df.age.cast('int'), df.salary.cast('int'))
df4.show()
df4.printSchema()

# like function

df5 = df.filter(df.gender.like('f%'))
df5.show()



