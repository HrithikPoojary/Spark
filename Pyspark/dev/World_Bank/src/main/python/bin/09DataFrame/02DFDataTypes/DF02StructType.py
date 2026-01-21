from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.types import StructType,StructField,StringType , IntegerType #type:ignore

spark = SparkSession.builder\
	.appName("Type")\
	.master("local[*]")\
	.getOrCreate()


print("---------Normal Way Creating DF-----------")

data = [("Luffy" , 19) , ("Zoro" , 21)]
schema = ["Name" , " Age"]

df1 = spark.createDataFrame(data = data , schema = schema )
df1.show()
df1.printSchema()

print("--------Using StructType-----------------")
schema1 = StructType ([
	StructField(name= "Name" ,dataType= StringType() , nullable=True),
	StructField(name = "Age" , dataType = IntegerType() , nullable= False)
])
df2 =  spark.createDataFrame(data = data , schema = schema1)
df2.show()
df2.printSchema()
