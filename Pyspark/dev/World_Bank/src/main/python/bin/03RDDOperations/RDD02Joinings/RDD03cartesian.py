from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

# Cartesian axa
rdd = spark.sparkContext.parallelize([1,3,2])
for i in rdd.cartesian(rdd).collect():
        print(i) 

print(f"Sorted {sorted(rdd.cartesian(rdd).collect())}")
print(f"Reverse Sorted {sorted(rdd.cartesian(rdd).collect(),reverse=True)}")