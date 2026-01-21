from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
	.appName('Help')\
	.master('local[*]')\
	.getOrCreate()


#print(help(spark))


# to get the version 

print(spark.sparkContext.version)
