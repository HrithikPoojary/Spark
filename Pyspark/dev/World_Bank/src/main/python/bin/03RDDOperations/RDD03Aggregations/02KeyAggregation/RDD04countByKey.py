from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('CountByKey')\
        .master('local[*]')\
        .getOrCreate()

rddpair = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

rddSplit = rddpair.map(lambda x : (x.split(',')[3] , 1))

rddcount = rddSplit.countByKey()
 
print('------Return type for countByKey <Dict>----------------')
print(type(rddcount))
print('------To Query countByKey <Dict>----------------')
for i,j in rddcount.items():
        print(i,j)