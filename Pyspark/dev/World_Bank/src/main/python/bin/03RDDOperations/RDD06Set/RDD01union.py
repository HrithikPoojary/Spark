from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('Set')\
	.master('local[*]')\
	.getOrCreate()

rddOrd = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

#To get the july month wdata

rddJuly = rddOrd.filter(lambda x : x.split(',')[1].split('-')[1]=='07')\
        	.map(lambda x : x.split(',')[2])

print(f'July Order COnut - {rddJuly.count()}')
 

rddAug = rddOrd.filter(lambda x : x.split(',')[1].split('-')[1]== '08')\
		.map(lambda x :x.split(',')[2])

print(f'August Order Count {rddAug.count()}')

rddUnion = rddJuly.union(rddAug)

print(f'Union count - {rddUnion.count()}') 

rddDistinct = rddUnion.distinct()

print(f'Distinct value - {rddDistinct.count()}') 

