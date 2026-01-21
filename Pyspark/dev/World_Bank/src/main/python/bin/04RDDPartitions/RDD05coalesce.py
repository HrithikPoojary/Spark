from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

rdd = spark.sparkContext.textFile('/practice/retail_db/orders',5)

print(f'Number of Partitions {rdd.getNumPartitions()}')

print(f'Count of each partitions {rdd.glom().map(len).collect()}')

rdd = rdd.coalesce(3)


print('--------------After Coalesce Each partition count---------------------')
print(rdd.glom().map(len).collect())
# Looks like 1 and 2 partitions are merged
# 3 and 4 are merged
# and 5 is left over

print('---------By default shuffle= False If you give higher value than existing one it will do nothing-------------')

#Current value 3

rdd = rdd.coalesce(10)
print(f'Current 3 passed 10 get num partitions {rdd.getNumPartitions()}')


print('---------if you modify parameter shuffle = true  then this will act as repartition-------------')

rdd = rdd.coalesce(10,shuffle = True)
print(f'Current 3 passed 10 get num partitions {rdd.getNumPartitions()}')

print('---------shuffle = true act as repartition records willl be uniformly distributed-------------')

print(rdd.glom().map(len).collect())
