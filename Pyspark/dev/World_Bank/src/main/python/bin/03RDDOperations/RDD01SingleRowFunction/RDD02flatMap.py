from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

rdd_ord = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

# FlatMap will consider ',' seprated value as element
# Map will consider each line as a element
# examples given below

#flatMap
print('------------------flatMap----------------------------------')
for i in rdd_ord.flatMap(lambda x : x.split(',')).take(5):
	print(i)
print('------------------Map----------------------------------')
#map
for i in rdd_ord.map(lambda x : x.split(',')).take(5):
        print(i)

# To slove wordcount like (closed,20) ('payment_status',3)
# flatMap - To get each items
# map     - To map each item to value to 1
# reduceByKey - to aggregate the value
print('------------------World count----------------------------------')
rdd_flat = rdd_ord.flatMap(lambda x : x.split(','))\
                   .map(lambda x : (x,1))\
                   .reduceByKey(lambda x,y : x+y)

for i in rdd_flat.take(5):
        print(i)
