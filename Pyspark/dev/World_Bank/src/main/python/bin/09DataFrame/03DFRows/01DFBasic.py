from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql import Row   #type:ignore

spark = SparkSession.builder\
	.appName('Row')\
	.master('local[2]')\
	.getOrCreate()

# Row - class and row - object of a class
row  = Row(name  = 'Luffy' , age = 17)

# New class row1
row1 = Row("name","age")

#Details
print(f'{row} and type {type(row)}')

#To Access individual row .

print(f' . name = {row.name} and age = {row.age}')

#To access individual row []
print(f' [] name = {row["name"]} and age = {row["age"]}')

#to ckeck where fields are present or not 
print(f'To check the field name - {"name" in row}')
print(f"To check the field bountry - {'bounty' in row}")


#to check the values are present in the field

print(f"To check the value present Luffy -  {'Luffy' in row.name}")
print(f"To check the value present Zoro -  {'Zoro' in row.name}")

