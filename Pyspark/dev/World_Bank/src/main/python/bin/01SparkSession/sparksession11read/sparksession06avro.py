from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
        .master('local[2]')\
        .appName('Read')\
        .getOrCreate()
'''
To create avro we need external data source
Read and write should be via external package...
 > spark-submit   --packages org.apache.spark:spark-avro_2.12:3.5.0   sparksession06avro.py




df = spark.read.load(path = '/practice/retail_db/orders_parquet'\
			, format = 'parquet')

df.write\
	.format('avro')\
	.mode('append')\
	.option('compression', 'snappy')\
	.save('/practice/retail_db/orders_avro')

'''

df = spark.read.load(path = '/practice/retail_db/orders_avro'\
			, format = 'avro')

df.show(5)

df.printSchema()

spark.stop()
