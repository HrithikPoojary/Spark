from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Local')\
        .master('local[2]')\
        .getOrCreate()
# From local file
with open('/home/hrithik_poojary/sample.txt', 'r') as f:
        # Convert text file to python list
        data = f.read().splitlines()
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

# to create new rdd for old rdd
new_rdd = rdd
# Different ways
new_rdd = rdd.map(lambda x :x[0]*2)
print(f"New RDD {type(new_rdd)}")
print(new_rdd.take(5))
        