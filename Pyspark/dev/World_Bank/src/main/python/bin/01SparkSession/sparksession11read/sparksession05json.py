from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
        .master('local[2]')\
        .appName('Read')\
        .getOrCreate()


'''
To create json file

df_json = spark.read.load(path = '/practice/retail_db/orders_parquet'\
				, format = 'parquet')


df_json.write\
	.mode('append')\
	 .option('multiline' , 'true')\
	   .json('/practice/retail_db/orders_json')

hadoop fs -head /practice/retail_db/orders_json/<json file name>

'''
df_json = spark.read.load(path = '/practice/retail_db/orders_json'\
			   , format = 'json')

df_json.show(5)

df_json.printSchema()


spark.stop()
