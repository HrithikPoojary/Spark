from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('Sort By Key')\
        .master('local[1]')\
        .getOrCreate()

rddOrd = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

# sortByKey will take (k,v) using key it will sort

print('-----------Converting (k,v)-------------------------')

rddPair = rddOrd.map(lambda x : (int(x.split(',')[2]),x))
for i in rddPair.take(5):
        print(i)

rddSort = rddPair.sortByKey(ascending = True)
print('-----------Sort By Key-------------------------')
for i in rddSort.take(10):
        print(i)


rddSortedRecords = rddSort.map(lambda x :x[1])
print('-----------sorted Records-------------------------')
for i in rddSortedRecords.take(5):
        print(i)

rddpair = rddOrd.map(lambda x : ((int(x.split(',')[2]) , x.split(',')[3]),x) )
print('-----------Two Keys First it will ascend by first key if equal then it will check the second key-------------------------')
rddSort = rddpair.sortByKey(ascending=True)
for i in rddSort.take(10):
        print(i)