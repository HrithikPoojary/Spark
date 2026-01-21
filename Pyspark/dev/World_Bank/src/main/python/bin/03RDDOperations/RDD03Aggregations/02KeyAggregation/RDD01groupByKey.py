from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

rdd_key = spark.sparkContext.textFile('/practice/retail_db/order_items/part-00000')

#******It doesn't use combiner
#should be in key-value pair this will group by key then it make values iterable, we can apply some function on it
rdd_group = rdd_key.map(lambda x : (int(x.split(',')[2]),float(x.split(',')[4]))).groupByKey()

# This will take key and apply the function on values
# example if two same keys are there this will one key apply function and next key this is not a group by 
# to get group by concept we will use groupByKey which will make values as iterables       

print('----------------Using mapValues------------------------------')
rdd_group_map = rdd_group.mapValues(sum)
for i in rdd_group_map.take(5):
        print(i)
print('----------------Using map------------------------------')
rdd_group_map = rdd_group.map(lambda x : (x[0],round(sum(x[1]),2)))
for i in rdd_group_map.take(5):
        print(i)

print('----------------Sorted------------------------------')
rdd_group_map = rdd_group.map(lambda x : (x[0],round(sum(x[1]),2)))
print(sorted(rdd_group_map.collect()))
