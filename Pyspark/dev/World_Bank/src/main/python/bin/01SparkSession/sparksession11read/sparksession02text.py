from pyspark.sql import SparkSession  # type:ignore

spark = SparkSession.builder\
	.appName('Read')\
	.master('local[2]')\
	.getOrCreate()


#print(help(spark.read.text))

print('---------------Text each row consider as one record---------------------------')
df = spark.read.load(path = '/practice/retail_db/orders'
			,format = 'text'
			  )

df.show(5 ,truncate = False)

print('-------------With whole text consider as one record------------------')

df = spark.read.load(path = '/practice/retail_db/orders' \
			,format = 'text' \
			  ,wholeText = True)

df.show(1,truncate = True)
