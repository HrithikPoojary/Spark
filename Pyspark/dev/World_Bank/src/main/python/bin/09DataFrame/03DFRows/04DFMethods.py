from pyspark.sql import SparkSession #type:ignore
from pyspark.sql import Row #type:ignore

spark = SparkSession.builder\
	.appName('Row')\
	.master('local[*]')\
	.getOrCreate()


data = Row(user = "Luffy" , age = 25 , userName = "Luffy")

print(f'Luffy occurance - {data.count("Luffy")}')

print(f'Zoro occurance - {data.count("Zoro")}')

print(f'25 occurance - {data.count(25)}')


print('----------to see the data stored-----------')

print(data)

print('---------To check the first occurance of the element position---------------')
print(data.index(25))


print(data.index('Luffy'))

print('----------convert value as disctionary-----------------')

print(data.asDict())
