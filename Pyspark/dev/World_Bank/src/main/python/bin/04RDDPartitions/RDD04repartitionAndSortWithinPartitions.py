from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

rdd = spark.sparkContext.parallelize(((9,('a','z')),(3,('x','f')),
                                      (6,('g','b')),(4,('a','b')),(8,('s','b')),(1,('a','b'))),2)

print('---------To View the Data-------------')
for i in rdd.collect():
        print(i)

print('-------To see the each partition data-----')

for i in rdd.glom().collect():
        print(i)

#repartitionAndSortWithinRepartitions(numPartitions =2(2 partition will be created) 
#                                    ,partitionFunc = <function portable_hash> (how to distribute the  values within partition)
#                                                       in this example true goes to one partition and false goes to another
#                                    ,ascending = True or False,
#                                     keyFunc)

# even one partition and odd one partition
rdd1 = rdd.repartitionAndSortWithinPartitions(numPartitions = 2,
                                                 partitionFunc =lambda x : x % 2,
                                                 ascending = True)
print('-------After repartitionAndSortWithinPartitions-----')
for i in rdd1.glom().collect():
        print(i)

# for 3 partitions we can use lambda x : x % 3    remainder[0,1,2]
#                             lambda x : x % 2    remainder[0,1] 