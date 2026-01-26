from pyspark.sql import SparkSession
from pyspark.sql.functions import sha1 ,sha2 , hash

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

#sha1(col) - used to encrypt string column if we want to use int we have to convert first
# 40 bit encrytion value len(sha1())
#returns hexa string sha1 family
print('---------sha1-------------')
df.select(df.dept , sha1(df.dept)).show(truncate = False)
df.select(df.age , sha1(df.age.cast("string")).alias("encrypted_age")).show(truncate = False)

#sha2(col , numBits) numbits = [0, 224, 256, 384, 512]
#return hexa string sha2 family
#64 bit encryption value len(sha2())
print('------sha2-------------')
df.select(df.age , sha2(df.age.cast("string"),0).alias("enc_age")).show(3,truncate = False)
df.select(df.age , sha2(df.age.cast("string") , 384).alias("en_age")).show(3,truncate = False)



#hash(*col) - creates some kind of a integer vale
print('-------hash-------------')
df.select(df.age , hash(df.age)).show(truncate = False)
df.select(df.age , hash(df.age , df.salary)).show(truncate = False)







spark.stop()

