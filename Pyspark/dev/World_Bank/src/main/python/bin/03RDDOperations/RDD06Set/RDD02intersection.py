from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('Set')\
	.master('local[*]')\
	.getOrCreate()

rddOrd = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

#To get the july month wdata

rddJuly = rddOrd.filter(lambda x : x.split(',')[1].split('-')[1]=='07')\
        	.map(lambda x : x.split(',')[2])

print(f'July Order count - {rddJuly.count()}')
 

rddAug = rddOrd.filter(lambda x : x.split(',')[1].split('-')[1]== '08')\
		.map(lambda x :x.split(',')[2])

print(f'Aug Order count -  {rddAug.count()}')

rddIntersect = rddJuly.intersection(rddAug) 

print(f'Common in between jul and aug and no duplicates - {rddIntersect.count()}')

print('For Example')

rdd1 = spark.sparkContext.parallelize([1,2,3,3,3])
rdd2 = spark.sparkContext.parallelize([1,4,3])

rdd3 = rdd1.intersection(rdd2)

print(f'Common between {rdd1.collect()} and {rdd2.collect()} - {rdd3.collect()}' )

