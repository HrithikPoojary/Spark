from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('Conf')\
        .master('local[*]')\
	.config('spark.default.parallelism' ,150)\
	.config('spark.sql.shuffle.partitions' , 100)\
	.config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()


# this will not affect the parallesim default because spark session is already created, we declare it while creating spark session
#spark.conf.set("spark.default.parallelism" , 150)
print(f"Default parallelism {spark.conf.get('spark.default.parallelism' )}")

rdd = spark.sparkContext.parallelize(range(10000))

print(f"Partitions{rdd.getNumPartitions()}")


#spark.conf.get('spark.sql.shuffle.partitions')
#this is only applicable to DataFrame

print(f"Shuffle partition  default - {spark.conf.get('spark.sql.shuffle.partitions')}")

data = [('Luffy' ,19) , ('Zoro' , 21 ) , ('Nami' , 20) , ('Luffy',30) , ('Ussop',21) , ('Zoro',25) , ('Sanji' , 20) , ('Chopper' , 15)]

df = spark.createDataFrame(data = data ,schema = ['name' , 'age'])

df1 = df.groupBy(df.name).count()

df1.show()
print(f"Shuffle - {df1.rdd.getNumPartitions()} ")











spark.stop()





