from pyspark.sql import SparkSession #type:ignore
from operator import add

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

rdd = spark.sparkContext.parallelize([('a',1),('b',1),('a',2)])
# It use Combiner so it will do data locality.
print('------------------Normal use-----------------')
rdd_reduce = rdd.reduceByKey(add)
print(rdd_reduce.collect())


print('--------------Each Order Revenue(add Python function)---------------------')
rdd = spark.sparkContext.textFile('/practice/retail_db/order_items/part-00000')
rdd_reduce = rdd.map(lambda x:(int(x.split(',')[1]),float(x.split(',')[4]))).reduceByKey(add)
for i in rdd_reduce.take(5):
        print(i)

print('--------------Each Order Revenue(lambda)---------------------')
rdd_reduce = rdd.map(lambda x:(int(x.split(',')[1]),float(x.split(',')[4]))).reduceByKey(lambda x,y : x+y)
for i in rdd_reduce.take(5):
        print(i)
print('------------Each Order Revenue(Cross Check for order Id 2)-----------------')
print(rdd.filter(lambda x : int(x.split(',')[1])==2).collect())

print('------------For Single Order Max Revenue(lambda)--------------')
rdd_reduce = rdd.map(lambda x : (int(x.split(',')[1]),float(x.split(',')[4]))).reduceByKey(lambda x,y :x if x>y else y)
for i in rdd_reduce.take(5):
        print(i)

print('------------For Single Order Max Revenue(max)--------------')
rdd_reduce = rdd.map(lambda x : (int(x.split(',')[1]),float(x.split(',')[4]))).reduceByKey(max)
for i in rdd_reduce.take(5):
        print(i)

print('---------For Single Order Max Revenue(to get the entire max row)----------')
rdd_reduce = rdd.map(lambda x : (int(x.split(',')[1]),x))\
        .reduceByKey(lambda x,y : x if (float(x.split(',')[4]) > float(y.split(',')[4])) else y )
for i in rdd_reduce.take(5):
        print(i)
        
