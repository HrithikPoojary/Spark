from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
        .master('local[1]')\
        .appName('RDD Creation')\
        .getOrCreate()

# print(help(spark.sparkContext.textFile))
rdd = spark.sparkContext.textFile('/practice/sample.txt')
for i in rdd.take(5):
        print(i)
# print(rdd.take(5))
print(f"Number of Partitons {rdd.getNumPartitions()}")

#Number records in each partitons
print(type(rdd.glom().collect()))
print(len(rdd.glom().collect()))
print(rdd.glom().map(len).collect())
