from pyspark.sql import SparkSession
from pyspark.sql.types import MapType,StringType,ArrayType, IntegerType ,StructType , StructField
from pyspark.sql.functions import json_tuple

spark = SparkSession.builder\
	.appName("Json")\
	.master("local[*]")\
	.getOrCreate()


data = [(1, """{"Zipcode" : 85016 , "ZipCodeType" : "STANDARD" , "City" :"Phoenix" , "State" : "AZ"}""")]
df_map = spark.createDataFrame(data = data , schema = ['id' , 'value'])

data = [(1,"""[1,2,3]""")]
df_arr = spark.createDataFrame(data = data ,schema = ['id' , 'value'])

data = [(1, """{"Zipcode" : 85016 , "ZipCodeType" : "STANDARD" , "City" :"Phoenix" , "State" : "AZ"}""")]
df_struct = spark.createDataFrame(data = data , schema = ['id' , 'value'])


#json_tuple(col , *fields)
print('------json_tuple--------------')
#custom column will be picked up to fix can use to	DF
df_map.select(json_tuple(df_map.value , "Zipcode","City")).show()
df_map.select(json_tuple(df_map.value , "Zipcode","City")).toDF("Zip" ,"City").show()
