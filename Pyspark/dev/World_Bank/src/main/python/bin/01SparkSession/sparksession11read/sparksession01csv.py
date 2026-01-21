from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
        .master('local[2]')\
        .appName('Read')\
        .getOrCreate()

# print(help(spark.read.csv))


print('--------------With schema-------------------------------------')
df = spark.read.load(path = '/practice/retail_db/orders/' \
		      ,format = 'csv'\
			,sep = ','\
			 ,schema = ('order_id int ,order_date string,order_customer_id int,order_status string')
		      )

df.show(5,truncate= False)

df.printSchema()

print('-----------------Without schema  all schema type will string-------------------------------')


df = spark.read.load(path = '/practice/retail_db/orders'\
				,format = 'csv'
				  ,sep = ',')

df.show(5,truncate = False)
df.printSchema()

print('-------------------With inferSchema-------------------------------------')

df = spark.read.load(path='/practice/retail_db/orders'
			,format = 'csv'
			  ,sep=','
			    , inferSchema = True)
df.show(5,truncate=False)
df.printSchema()

print('--------------------Header------------------------------')

df = spark.read.load(path = '/practice/retail_db/orders'
			,format = 'csv'
			  , sep = ','
			    , inferSchema = True
			      ,header = True)


df.show(5,truncate = False)
df.printSchema()

spark.stop()
