from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.functions import udf #type:ignore
from pyspark.sql.types import StringType #type:ignore

spark = SparkSession.builder\
	.appName('SQL')\
	.master('local[2]')\
	.getOrCreate()


# We cannot directly use python user defined function in df and spark sql we have use udf

@udf(returnType = StringType())
def initCap(str):                       # I am spark developer
	finalstr = ''
	ar = str.split(' ')             #['I' , 'am' ,'spark' , 'developer']
	for i in ar:
		finalstr =  finalstr + i[0:1].upper() + i[1:] + ' '         # I Am Spark Developer
	return finalstr.strip()

lst =  [(19,"monkey d luffy") , (21 , "roronoa zoro")]

df = spark.createDataFrame(data = lst , schema= ['age','name'])

df.select(df.name).show()

df.select(df.name , initCap(df.name)).show()

# To use udf in spark sql we have to register first

spark.udf.register("initcap",initCap)

spark.sql('''select initcap("I am sql developer")''').show()

df.createOrReplaceTempView('pirate')

spark.sql(''' select name, initcap(name) as new_Name from pirate''').show()
