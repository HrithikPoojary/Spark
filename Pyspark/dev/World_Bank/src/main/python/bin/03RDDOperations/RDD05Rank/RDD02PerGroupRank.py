from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('Rank')\
        .master('local[*]')\
        .getOrCreate()

rdd = spark.sparkContext.textFile('/practice/retail_db/products/part-00000')

rdd = rdd.filter(lambda x : (int(x.split(',')[1]) in [2,3,4]) and (int(x.split(',')[0]) in [1,2,3,4,5,25,26,27,28,29,49,50,51,52,53]))

rddGroupBy = rdd.map(lambda x : (x.split(',')[1],x)).groupByKey()
print('--------Group by Iterable----------------')
for i in rddGroupBy.take(5):
        print(i)

print('--------Analyze First Record-----------')
first = rddGroupBy.first()

print(first)
print('--------Sorted First Record First Record-----------')
sortedFirst = sorted(first[1],key= lambda x : -(float(x.split(',')[4])),reverse=True)
print(sortedFirst)

rddFlatMap = rddGroupBy.flatMap(lambda x : sorted(x[1],key= lambda k : -(float(k.split(',')[4])),reverse=True) [:2])
print('--------Items which has two high price (flapMap)-----------')
for i in rddFlatMap.collect():
        print(i)

rddFlatMap = rddGroupBy.map(lambda x : sorted(x[1],key= lambda k : -(float(k.split(',')[4])),reverse=True) [:2])
print('--------Items which has two high price (map)-----------')
for i in rddFlatMap.collect():
        print(i)