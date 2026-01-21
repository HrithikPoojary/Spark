from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
	.appName('Create Dataframe')\
	.master('local[*]')\
	.getOrCreate()

lst = [("Luffy",19),("Zoro",21)]

rdd = spark.sparkContext.parallelize(lst)



for i in rdd.take(2):
	print(i)

df = spark.createDataFrame(data = rdd , schema = ('Name string , Age Integer'))

df.show()

df.printSchema()
