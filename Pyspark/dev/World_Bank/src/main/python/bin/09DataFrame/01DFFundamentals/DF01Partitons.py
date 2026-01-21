from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('Partitons')\
	.master('local[2]')\
	.getOrCreate()


lst = [('Luffy', 19),('Zoro', 21) ,('Nami' , 20),('Ussop',18)]

df = spark.createDataFrame(data = lst , schema = 'Name string , Age int ')

df.show()

# default partitions depends on local[*] - > 4, local[2] - >2
# Normal default will be 2

print(f'Partitions - {df.rdd.getNumPartitions()}')
print(f'Parallelism - {spark.sparkContext.defaultParallelism}')
