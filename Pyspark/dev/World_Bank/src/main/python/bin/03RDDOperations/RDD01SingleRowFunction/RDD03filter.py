from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

rdd_ord = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

#Filter only closed and complete status
print('--------------------only closed and complete-----------------------------------')
rdd_filter = rdd_ord.filter(lambda x : x.split(',')[3] in ('CLOSED','COMPLETE'))
for i in rdd_filter.take(5):
        print(i)

#Filter only 2014
print('--------------------only 2024-----------------------------------')
rdd_filter = rdd_ord.filter(lambda x : x.split(',')[1].split(' ')[0].split('-')[0]=='2014')
for i in rdd_filter.take(5):
        print(i)

#Filter only 2014
print('--------------------combine filter one and two-----------------------------------')

rdd_filter = rdd_ord.filter(lambda x : ((x.split(',')[3]) in ['CLOSED','COMPLETE'])\
                        and (x.split(',')[1].split(' ')[0].split('-')[0]=='2014') )
for i in rdd_filter.take(5):
        print(i)

