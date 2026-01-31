from pyspark.sql import SparkSession

spark = SparkSession.builder\
	.appName('coalesce')\
	.master('local[*]')\
	.getOrCreate()

df = spark.read.load(path ='/practice/test/',format = 'csv' , sep = ',', schema = 'col1 int ,col2 int , col3 int')

df.printSchema()


df = df.where(df.col1 < 501)

print(df.count())

print(df.rdd.glom().map(len).collect())


df = df.coalesce(2)

print(df.rdd.glom().map(len).collect())

df = df.rdd.coalesce(3,shuffle = True)

print(df.glom().map(len).collect())
