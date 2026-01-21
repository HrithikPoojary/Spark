from pyspark.sql import SparkSession   #type:ignore
from pyspark.sql.functions import sum   #type:ignore
from time import time
 
spark = SparkSession.builder\
	.appName('Accumulator')\
	.master('local[2]')\
	.getOrCreate()


print(f'Default setting {spark.conf.get("spark.sql.codegen.wholeStage")} ')

spark.conf.set("spark.sql.codegen.wholeStage",False)

print(f'After setting the wholeStage False result {spark.conf.get("spark.sql.codegen.wholeStage")}')

print(f'Currnet Time - {time()}')

# Set whole stage code generation to false that old version 1.6 version

spark.conf.set("spark.sql.codegen.wholeStage",False)

print("---------------Code generation wholestage False old version before 1.6------------------------")

def benchMark(version):
	start = time()
	spark.range(1000*1000*1000).select(sum("id")).show()
	end = time()
	elapsed = end-start
	print(elapsed)

benchMark("1.6 Version")
print('\n------------Code generation wholeStage True after 2.0---------------------------')

spark.conf.set("spark.sql.codegen.wholeStage",True)

def benchMark(version):
        start = time()
        spark.range(1000*1000*1000).select(sum("id")).show()
        end = time()
        elapsed = end-start
        print(elapsed)

benchMark("2.0 Version")

