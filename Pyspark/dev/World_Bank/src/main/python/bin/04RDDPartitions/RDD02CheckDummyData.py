from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

rdd = spark.sparkContext.textFile('/practice/test/sample')
#print(f'File rows {rdd.count()}')
print(f'File(32Million rows) number of partitons {rdd.getNumPartitions()}')

rdd1 = rdd.filter(lambda x : int(x.split(',')[0])==1)

# print(f'Filter row {rdd1.count()}')
print(f'After Filter rows(32 rows) number of partitons {rdd.getNumPartitions()}')

#we have to have manually adjust the partitions
rdd1 = rdd1.coalesce(1)
print(f'After coalesce number of partitons {rdd1.getNumPartitions()}')


