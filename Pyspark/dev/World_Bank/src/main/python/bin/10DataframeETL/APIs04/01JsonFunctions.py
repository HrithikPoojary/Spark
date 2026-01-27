from pyspark.sql import SparkSession
from pyspark.sql.types import MapType,StringType,ArrayType, IntegerType ,StructType , StructField
from pyspark.sql.functions import from_json ,to_json

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


print('--------Map------------')
df_map.show(truncate = False)
df_map.printSchema()

print('--------Array------------')
df_arr.show(truncate = False)
df_arr.printSchema()

print('--------Struct------------')
df_struct.show(truncate = False)
df_struct.printSchema()

# from_json - convert json string to map or struct or array type
# from_json(col ,schema)
# to_json - opsite of from_json
# to_json - this will convert from  map , array or struct type to json string

print('-----json string to maptype-----------')
schema = MapType(StringType() , StringType())
df_mapped = df_map.withColumn('map_column' , from_json(df_map.value , schema))
df_mapped.printSchema()

df_mapped.select(df_mapped.map_column['Zipcode']).show()

print('-------String array to  Arraytype---------------')
schema = ArrayType(IntegerType())
df_array = df_arr.withColumn('array_column' , from_json(df_arr.value , schema))
df_array.printSchema()
df_array.select(df_array.array_column[0]).show()


print('----------string map to struct type-------------------------')

schema = StructType(
[
	StructField("Zipcode" , IntegerType()),
	StructField("ZipcodeType" , StringType()),
	StructField("City" , StringType()),
	StructField("State" , StringType())
]
		    )

df_structed = df_map.withColumn('struct_column' , from_json(df_map.value , schema))
df_structed.printSchema()

df_structed.select(df_structed.struct_column.Zipcode).show()


print('--------to_json map to string----------')

df_mapped.select(df_mapped.map_column , to_json(df_mapped.map_column)).printSchema()


print('------to_json  array to json string----------------')

df_array.select(df_array.array_column,to_json(df_array.array_column)).printSchema()

print('-------to_json struct to string json------------------')

df_structed.select(df_structed.struct_column,to_json(df_structed.struct_column)).printSchema()



spark.stop()
