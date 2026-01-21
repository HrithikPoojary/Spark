from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Parallelize')\
        .master('local[2]')\
        .getOrCreate()

# Creating RDD using Python list / Parallelize operation
lst = [1,2,3,4,5,6,7,8,9,0]
rdd = spark.sparkContext.parallelize(lst)

print(rdd.take(5))
#Collect will print all the elements in the list.
print(rdd.collect())