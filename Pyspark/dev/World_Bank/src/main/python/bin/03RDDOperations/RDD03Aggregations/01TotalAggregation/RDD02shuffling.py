from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

rdd = spark.sparkContext.parallelize([1,2,3])
rdd1 = rdd.distinct()
print(rdd1.toDebugString())