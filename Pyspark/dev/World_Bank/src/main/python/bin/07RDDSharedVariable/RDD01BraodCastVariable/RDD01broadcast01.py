from pyspark.sql import SparkSession    #type:ignore

spark = SparkSession.builder\
	.appName('BroadCast')\
	.master('local[2]')\
	.getOrCreate()

# Broadcast Examples

days = {
	"sun":'Sunday',
	"mon" : 'Monday',
	"tue" : 'Tuesday'
	}

broadCastDays = spark.sparkContext.broadcast(days)

print(broadCastDays.value)
print(broadCastDays.value['sun'])


num = [1,2,3]

bcNumbers = spark.sparkContext.broadcast(num)

for i in bcNumbers.value:
	print(i)





