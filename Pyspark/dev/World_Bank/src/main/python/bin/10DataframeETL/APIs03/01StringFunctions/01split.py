from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , TimestampType
from pyspark.sql.functions import split

spark  = SparkSession.builder\
	.appName('String')\
	.master('local[*]')\
	.getOrCreate()

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
#split(col , regexp)


df.select(split(df.order_dt,'-').alias("order_dt_list")).show(5,truncate = False)

print('--------schema of slipt fun---------------')
df1 = df.select(split(df.order_dt,'-').alias("order_dt_list"))
df1.printSchema()

df.select(df.order_dt , split(df.order_dt , '-')[0].alias("order_dt") ).show(5 ,truncate=False)


print("-------real world problems--------------")

df_rw = spark.createDataFrame(data =[('ab12cd23ef27gh97',)], schema = ['id' , ] )

df_rw.show()
# [0-9] -> if any interger it will split   
df_rw.select(split(df_rw.id , '[0-9]+')).show()






spark.stop()
