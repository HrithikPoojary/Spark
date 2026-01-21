from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.functions import udf #type:ignore
from pyspark.sql.types import StringType #type:ignore

spark = SparkSession.builder\
	.appName('SQL')\
	.master('local[2]')\
	.getOrCreate()

def initCap(str):
        finalstr = ''
        ar = str.split(' ')
        for word in ar:
                finalstr = finalstr + word[0:1].upper() + word[1:] + ' '
        return finalstr

spark.udf.register("initcap",initCap)
spark.sql(''' select initcap(" i am plsql developer")''').show()

spark.stop()
