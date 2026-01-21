from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql import Row   #type:ignore

spark = SparkSession.builder\
	.appName('Row')\
	.master('local[2]')\
	.getOrCreate()

# person is a class now
Person = Row("name" , "age")

p1 = Person("Luffy" , 17)

p2 = Person('Zoro' , 21)

print(p1.name + ' ' + p2.name)

print('----RDD-----')
rdd = spark.sparkContext.parallelize([Person('Nami' , 20) , Person('Ussop' , 18)])

for i in rdd.collect():
	print(i.age)

print('----DF------')

data = [Person("Sanji" , 21) , Person("Chopper" , 12)]

df = spark.createDataFrame(data = data)

df.show()
df.printSchema()
