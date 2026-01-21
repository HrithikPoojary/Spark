from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

rdd_key_value = spark.sparkContext.parallelize([('a',[1,2,3]),('b',[4,5,6]),('a',[6,5])])

#mapValues  we will take key value pair using key it will perform operation on values(no group with key)
print('------------------Normal Function------------------------------------')
rdd_len = rdd_key_value.mapValues(sum)
for i in rdd_len.take(3):
        print(i)

print('------------------User Defined Function------------------------------')
def lenFunction(values):
        return len(values)
rdd_len = rdd_key_value.mapValues(lenFunction)
for i in rdd_len.take(3):
        print(i)