from pyspark.sql import SparkSession   #type:ignore


spark = SparkSession.builder\
	.appName('Range')\
	. master('local[*]')\
	.getOrCreate()

#print(help(spark.range))


df = spark.range(start = 1, end = 10, step = 2 ,numPartitions= 2)

print(df.rdd.getNumPartitions())

print(df)

df.show()

spark.range(20).show()
