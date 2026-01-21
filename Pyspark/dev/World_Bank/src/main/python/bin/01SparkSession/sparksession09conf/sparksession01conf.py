
from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
	.appName('SQL')\
	.master('local[2]')\
	.getOrCreate()

# print(help(spark.conf))

# Default set
print(spark.conf.get(key = 'spark.sql.shuffle.partitions'))

spark.conf.set(key = 'spark.sql.shuffle.partitions',value = 300)

# After set
print(spark.conf.get(key = 'spark.sql.shuffle.partitions'))


# YARN variable path

spark.conf.set(key = 'spark.yarn.appMasterEnv.HDFS_PATH' , value = 'practice/retail_db/orders')

print(f'Yarn Env Path - {spark.conf.get("spark.yarn.appMasterEnv.HDFS_PATH")}')

spark.conf.set(key = 'spark.yarn.appMasterEnv.USER' , value = 'Spark User')

print(f'Spark Env User - {spark.conf.get(key = "spark.yarn.appMasterEnv.USER")} ')



spark.stop()
