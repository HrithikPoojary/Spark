from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
        .master('local[2]')\
        .appName('ORC')\
        .getOrCreate()


'''
To  create parquet file

df = spark.read.load(path = '/practice/retail_db/orders_orc'\
			, format = 'orc')

df.write \
	.mode('overwrite')\
	  .option('compression' , 'snappy')\
		.parquet('/practice/retail_db/orders_parquet')
'''

df = spark.read.load(path = '/practice/retail_db/orders_parquet/'\
			, format = 'parquet')

df.show(5)


spark.stop()
