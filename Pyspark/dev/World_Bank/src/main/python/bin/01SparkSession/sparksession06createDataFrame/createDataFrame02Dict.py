from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
	.appName('Create Dataframe')\
	.master('local[*]')\
	.getOrCreate()

dict =  [ {"Name":"Luffy", "Age" : 19} ,
	  {"Name" :"Zoro" , "Age" : 21}	]

df = spark.createDataFrame(data = dict)

df.show()

df.printSchema()

