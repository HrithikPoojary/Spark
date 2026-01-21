from pyspark.sql import SparkSession   #type:ignore
from pyspark.sql.functions import sum   #type:ignore
from time import time
 
spark = SparkSession.builder\
	.appName('Accumulator')\
	.master('local[2]')\
	.getOrCreate()


# default wholeStage = true

spark.conf.set("spark.sql.codegen.wholeStage",True)

spark.range(1000).filter("id>100").selectExpr("sum(id)").show()

# explain  default  extended = False  we will get only physical plan

spark.range(1000).filter("id>100").selectExpr("sum(id)").explain()

# extended  = True

print('------------------Extended True-----------------------------')

spark.range(1000).filter('id>100').selectExpr('sum(id)').explain(extended = True)
