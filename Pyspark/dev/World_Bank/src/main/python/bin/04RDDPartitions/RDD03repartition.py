from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

#repatition will increase or decrease create new partition evenly(not perfectly) distributed records .more shuffling
# coalesce will only decrease the partition less shuffling the data to new partition 

rdd = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

print(f'Default Order file partitions {rdd.getNumPartitions()}')

#print(rdd.glom())
#print(rdd.glom().collect())
print(f'Each partitions record count {rdd.glom().map(len).collect()}')

rdd= rdd.repartition(5)
print(f'After Repartitions Order file partitions {rdd.getNumPartitions()}')
print(f'Each partitions record count {rdd.glom().map(len).collect()}')


