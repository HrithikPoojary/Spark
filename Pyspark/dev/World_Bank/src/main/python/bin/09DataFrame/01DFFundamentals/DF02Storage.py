from pyspark.sql import SparkSession  # type:ignore
from pyspark import StorageLevel #type:ignore


spark = SparkSession.builder\
	.appName('Storate')\
	.master('local[*]')\
	.getOrCreate()

df = spark.range(10)

print(df.rdd.getStorageLevel())

print('--------Storing it only in Memory----------')
df.rdd.persist()

print(df.rdd.getStorageLevel())
# to avoid below error if we try to persist the df twice in memory it will throw error
df.rdd.unpersist()

print('-------Storing it both memory and disk------------------')
df.rdd.persist(storageLevel = StorageLevel.MEMORY_AND_DISK_2) 
print(df.rdd.getStorageLevel())


spark.stop()
