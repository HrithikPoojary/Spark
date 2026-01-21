from pyspark.sql import SparkSession #type:ignore
from pyspark.sql import Row #type:ignore

spark = SparkSession.builder\
	.appName('Partitons')\
	.master('local[2]')\
	.getOrCreate()

data = [Row(Name = 'Luffy' , Age = 17 ),Row( Name = 'Zoro',Age = 21)]

rdd = spark.sparkContext.parallelize(data)

for i in rdd.collect() :
	print(i.Name + ' ' + str(i.Age))

# by default schema will picked from data
print('-------by default----------')
df = spark.createDataFrame(data=data)
df.show()
df.printSchema()

#We explicitly give schema
print('------explicit----------')
df1 = spark.createDataFrame(data = data , schema = ('Pirates string , Old int'))
df1.show()
df1.printSchema()

# Without any
rw = [Row('Name')]

df2 = spark.createDataFrame(data = rw)
df2.show()
