from pyspark.sql import SparkSession 

spark = SparkSession.builder\
	.appName('Repartition')\
	.master('local[*]')\
	.getOrCreate()


df = spark.range(1000000)
df = df.select(df.id , df.id*2 , df.id*3)

df = df.union(df)
df = df.union(df)
df = df.union(df)

df.write.mode("append").csv("/practice/test/" )

spark.stop()
