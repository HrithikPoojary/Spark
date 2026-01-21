from pyspark.sql import SparkSession   #type:ignore
 
spark = SparkSession.builder\
	.appName('Accumulator')\
	.master('local[2]')\
	.getOrCreate()

# creating function, inside that one global variable each time function calls global variable increase by 1

counter = 0
def  f1(x):
	global counter
	counter += 1

f1('First Call')
print(f'First Call {counter}')

f1('Second call')
print(f'Second Call {counter}')

# foreach, this function will assing every element of the rdd to function

rdd = spark.sparkContext.parallelize([1,2,3])

print(help(rdd.foreach()))

rdd.foreach(f1) # this function will call 3 times , counter expected result should 2 +  3 = 5  , result is 2

print(f'After foreeach function {counter}')

# beacuse  the each function f1 is executed in the executor locally and it change the value in local machine and it will not send back the changed value 
# to the driver program.For this kind of problem we need accumulator variable. if executor changes the value and it assign back the changed value to driver program


