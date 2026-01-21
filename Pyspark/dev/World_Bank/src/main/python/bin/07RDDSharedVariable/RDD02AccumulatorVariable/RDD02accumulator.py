from pyspark.sql import SparkSession   #type:ignore
 
spark = SparkSession.builder\
	.appName('Accumulator')\
	.master('local[2]')\
	.getOrCreate()

# deriving accumulator variable

rdd = spark.sparkContext.parallelize((1,2,3))
counter = spark.sparkContext.accumulator(0)

print(f'Initial value {counter}')

def f1(x):
	global counter
	counter.add(1)  # we cannot directly add like +=1 in accumulator instead we can use add function

rdd.foreach(f1)    # this will call the function thrice

print(f'After foreach accumalator set up {counter}')
 
print(counter.value)  # This values only accesed by driver program

accumulatorVar = spark.sparkContext.accumulator(0)

rdd = spark.sparkContext.parallelize([1,2,3,4,5])
# To sum the all the value

rdd.foreach(lambda x : accumulatorVar.add(x))

print(f'After add all Element {accumulatorVar.value}')

# Lazy evaluation

lazyAccum = spark.sparkContext.accumulator(0)

def f2(x):
	global lazyAccum
	lazyAccum.add(1)

rdd.map(f2)

print(f'Lazy evaluation using map {lazyAccum.value}')
