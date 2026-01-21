from pyspark.sql import SparkSession    #type:ignore

spark = SparkSession.builder\
	.appName('BroadCast')\
	.master('local[2]')\
	.getOrCreate()


days =  {
	"sun":'Sunday',
	"mon":'Monday',
	"tue":'Tuesday'
	}

bcDays = spark.sparkContext.broadcast(days)

data = [
	("Monkey", "Luffy" , "EastBlue","sun"),
	("Roronoa" , "Zoro", "EastBlue" , "mon"),
	("Nami","Swan","EastBlue","tue"),
	("God","Ussop","WestBlue","sun")
	]
rdd = spark.sparkContext.parallelize(data)


def days_convert(dict_key):
	return bcDays.value[dict_key]

input = rdd.map(lambda x : (x[0],x[1],x[2],x[3])).collect()

print('Input \n')

for i in input:
	print(i)

output = rdd.map(lambda x : (x[0],x[1],x[2],days_convert(x[3]))).collect()

print('\n  Output \n')

for i in output:
	print(i)
