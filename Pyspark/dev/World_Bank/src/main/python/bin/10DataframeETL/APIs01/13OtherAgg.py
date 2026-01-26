from pyspark.sql import SparkSession
from pyspark.sql.functions import  first , greatest ,skewness , collect_list


spark = SparkSession.builder\
	.appName('Window')\
	.master('local[*]')\
	.getOrCreate()

#print(help(window))

data = [
    ('James' , 'Sales','NY' , None , 34),
    ('Alicia' , 'Sales','NY' , 8600 , 56),
    ('Robert' , 'Sales','CA' , 8100 , 30),
    ('John' , 'Sales', 'AZ' , 8600 , 31),
    ('Rahul' , 'Sales', 'AZ' , 1000 , 31),
    ('Ross' , 'Sales','AZ' , 8100 , 33),
    ('Lisa' , 'Finance','CA' , 9000 , 24),
    ('Deja' , 'Finance','CA' , 9900 , 40),
    ('Sugie' , 'Finance','NY' , 7900 , 36),
    ('Charlie' , 'Finance','AZ' , 5000 , 36),
    ('Ram' , 'Finance','NY' , 8900 , 53),
    ('Kyle' , 'Marketing','CA' , 8000 , 25),
    ('Reid' , 'Marketing','NY' , 9100 , 50)
]

schema = ['empname', 'dept','state','salary','age']

df = spark.createDataFrame(data = data ,schema = schema)

#first(col , ignorenulls = False )
# last(col , ignorenulls = False)
print('-----------------First---------------------')
df.select(first(df.salary)).show()
df.select(first(df.salary , ignorenulls = True ) ).show()


#greatest(*col ) ignores null values 
#least(*col)
print('----------------greatest------------------------')
df.select (greatest(df.salary , df.age) ).show()

#skewness
print('---------------skewness  (to check the outliers)---------------------')
df.select(skewness(df.salary)).show()

#collect_list - keeps duplicates
#collect_set - removes duplicates
print('--------------collect_list------------------------')
df.select(collect_list(df.age)).show(truncate = False)
