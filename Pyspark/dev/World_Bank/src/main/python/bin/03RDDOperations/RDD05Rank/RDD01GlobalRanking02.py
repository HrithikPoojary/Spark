# takeOrdered(num (number of records like limit),key (order of execution))

from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('Rank')\
        .master('local[*]')\
        .getOrCreate()

rdd = spark.sparkContext.parallelize([10,5,7,9,11,25])
#takeOrdered Api takes two arguments num(mandatory) out rows default ascendgin & key -in which order
print('--------Ascending------------')  
print(rdd.takeOrdered(2))
print('--------------Descending--------------------')
print(rdd.takeOrdered(2,key = lambda x : -x))

rdd = spark.sparkContext.textFile('/practice/retail_db/products/part-00000')

rdd = rdd.filter(lambda x:x.split(',')[4] != '')

print('----------Same problem using takeOrdered-----------------')
for i in rdd.takeOrdered(10 , key = lambda k : -(float(k.split(',')[4]))):
        print(i)