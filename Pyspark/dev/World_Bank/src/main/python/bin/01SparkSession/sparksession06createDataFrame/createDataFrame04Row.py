from pyspark.sql import SparkSession   #type:ignore
from pyspark.sql import Row   #type:ignore

spark = SparkSession.builder\
	.appName('Create Dataframe')\
	.master('local[*]')\
	.getOrCreate()

row = Row(Name = "Nami"  ,Age = 20)

print(row.Name)
print(row.Age)

# to check the attribute is present or not

print('Name' in row)

rdd = spark.sparkContext.parallelize([Row(Name='Luffy',Age = 19),Row(Name='Zoro',Age = 21)])

for i in rdd.take(2):
	print(i)


df = spark.createDataFrame(data = rdd)

df.show()
