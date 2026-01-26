from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , TimestampType
from pyspark.sql.functions import translate ,overlay

spark  = SparkSession.builder\
	.appName('String')\
	.master('local[*]')\
	.getOrCreate()

'''
order_path = '/practice/retail_db/orders/part-00000'
schema = StructType(
[
	StructField("order_id" , IntegerType() , False),
	StructField("order_dt" , TimestampType() , False),
	StructField("cus_order_id" , IntegerType() , False),
	StructField("order_status" , StringType() , False),
]
)


df = spark.read.load(path = order_path ,
			format = 'csv' ,
			  sep = ',' ,
				schema = schema)
print('--------Working data-------------')
df.show(2)
'''

df = spark.createDataFrame(data= [('translate',)],schema = ['id' , ])

df.show()

#translate(strCol , matching , replace)
#overlay(srcCol, replaceCol , pos , len)

df.withColumn('ext' , translate(df.id , 'slat' , '1234')).show()
# t -> 4
# l -> 2
# a -> 3
# s -> 1
df.withColumn('ext' , translate(df.id , 'rnlt' , '123')).show()
# r -> 1
# n -> 2
# l -> 3
# t ->Null

df = spark.createDataFrame(data = [('SPARK_SQL' ,'CORE')] , schema = ['src_x' , 'replace_y'])

df.select(df.src_x , df.replace_y , overlay(df.src_x , df.replace_y, 7 ).alias("overlayed")).show()
#len  -> at the end of the string
#SPARK_SQL
#1234567   ->SQL  <-> CORE   ==> SPARK_CORE








spark.stop()






