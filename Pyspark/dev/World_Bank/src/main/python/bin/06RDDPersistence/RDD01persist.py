from pyspark.sql import SparkSession  #type:ignore
from pyspark   import StorageLevel    #type:ignore

spark = SparkSession.builder\
	.appName('Persistence')\
	.master('local[2]')\
	.getOrCreate()

rdd = spark.sparkContext.parallelize(('a','c','b',))

#To check rdd is persisted or not

print(rdd.is_cached)


# To Persist the rdd

rdd.persist()


# Check again after persist method

print(rdd.is_cached)

# to check starage level

print(rdd.getStorageLevel())

# After the persist if you try to modify the storage level it will thrown an exception

# rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK_2)   we will get below error

#pyspark.errors.exceptions.captured.UnsupportedOperationException: Cannot change storage level of an RDD after it was already assigned a level

rdd1 = spark.sparkContext.parallelize(('a','b','c'))

rdd1.persist(storageLevel = StorageLevel.MEMORY_AND_DISK_2)

print(rdd1.getStorageLevel())

















