from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
	.appName('Create Dataframe')\
	.master('local[*]')\
	.getOrCreate()


# print(help(spark.createDataFrame))

lst = [("Luffy", 19) , ("Zoro" , 21)]

# Without schema name column we get _1,_2,_3 so on......

df = spark.createDataFrame(data = lst)

df.show()

df.printSchema()

# With Schema

df = spark.createDataFrame(data = lst , schema = ["Name","Age"])

df.show()

df.printSchema()

# Schema type

df = spark.createDataFrame(data = lst , schema = ('Name string , Age Int'))

df.show()

df.printSchema()
