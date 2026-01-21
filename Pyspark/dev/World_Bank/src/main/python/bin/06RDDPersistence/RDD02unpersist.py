from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
	.appName('Unpersistence')\
	.master('local[2]')\
	.getOrCreate()

rdd =  spark.sparkContext.parallelize(('a','b','c'))

print('---------------------- To check rdd is persisted or not ----------------' )

print(rdd.is_cached)

print('-----------------To Persist rdd ------------------------')

rdd.persist()

print('---------------------To check rdd is persisted oor not after applying the persist method---------------------')

print(rdd.is_cached)

print('------------------to unpersist------------------------')

rdd.unpersist()

print('---------------------To check rdd is persisted oor not after applying the persist method-----------')

print(rdd.is_cached)


