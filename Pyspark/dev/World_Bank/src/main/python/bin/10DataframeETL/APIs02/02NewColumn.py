from pyspark.sql import SparkSession 
from pyspark.sql.functions import spark_partition_id , rand , randn


spark = SparkSession.builder\
	.appName('Window')\
	.master('local[*]')\
	.getOrCreate()

#print(help(window))

data = [
    ('James' , 'Sales','NY' , 9000 , 34),
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

print('-------spark_partition_id---------------')
df1 = spark.range(10)
df1 = df1.repartition(5)
print(df1.rdd.getNumPartitions())
df1.select(df1.id , spark_partition_id()).show()


print('---------rand(uniformly distributed)  value( 0 ,1 )-----------------')
df.select(rand(seed  = 70)).show()

print('---------randn(stardard deviation) value (-infinite , +infnite) majority (-3 , +3)-----------------------')
df.select(randn(seed = 70)).show()


