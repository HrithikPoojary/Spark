from pyspark.sql import SparkSession #type:ignore
from pyspark.sql.functions import udf  #type:ignore
from pyspark.sql.types import StringType,IntegerType #type:ignore


spark = SparkSession.builder\
	.appName('NewSession')\
	.master('local[2]')\
	.getOrCreate()

new_spark = spark.newSession()
print(spark)
print(new_spark)
 
@udf(returnType = StringType())
def initCap(str):
	finalstr = ''
	ar = str.split(' ')
	for word in ar:
		finalstr = finalstr + word[0:1].upper() + word[1:] +' '
	return finalstr.strip()

lst = [('i am a monkey d luffy future pirate king' , )]
df =  spark.createDataFrame(data = lst , schema = ('Name string'))

df.select(df.Name,initCap(df.Name)).show(truncate = False)

spark.udf.register('my_initcap' , initCap)

print('-----------Spark Session---------------')
spark.sql('''select my_initcap(' i am a roronoa zoro future strongest swords man') as Name''').show(truncate= False)

print('-----------newSession  (we cannot access udf via new session)--------------')

# Error
# new_spark.sql('''select my_initcap(' i am a roronoa zoro future strongest swords man') as Name''').show(truncate= False)



spark.stop()
new_spark.stop()
