from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

x = spark.sparkContext.parallelize([('a',1),('b',2)])
y = spark.sparkContext.parallelize([('a',4)])

xy = x.cogroup(y)
print('-------------------Normal COLLECT of COGROUP------------------')
print(xy.collect())

print('-------------------To Solve ITERABLE Using COLLECT------------------')
for x , (y,z) in xy.collect():
        print(x,list(y),list(z))

print('-------------------Normal TAKE of COGROUP------------------')
for i in xy.take(2):
        print(i)

print('-------------------To Solve ITERABLE TAKE------------------')
# Using Python Map Function map(f,iterable)
for i,j in xy.take(2):
        print (i,list(map(list,j)))

