from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.functions import udf #type:ignore
from pyspark.sql.types import IntegerType #type:ignore

spark = SparkSession.builder\
	.appName('SQL')\
	.master('local[2]')\
	.getOrCreate()

len_str = udf(lambda str: len(str) , returnType=IntegerType())

spark.udf.register("lenStr" , len_str)

spark.sql('''
		select lenStr("spark") as lenLetter
		''').show()


lst = [("monkey d luffy" , 21) , ("roronoa zoro" , 23)]
df = spark.createDataFrame(data = lst , schema = ['Name','Age'])

df.createOrReplaceTempView('pirates')

spark.sql('''
		select name , lenStr(name) as pirateLen from pirates
	''').show()

spark.stop()
