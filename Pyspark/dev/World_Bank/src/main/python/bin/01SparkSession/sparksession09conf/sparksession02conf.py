# run using spark-submit --conf 'spark.yarn.appMasterEnv.HDFS_PATH'='/practice/retail_db/orders' --conf 'spark.sql.shuffle.partitions'=300 sparksession02conf.py

from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
	.appName('SQL')\
	.master('yarn')\
	.getOrCreate()


noPartitions = spark.conf.get(key='spark.sql.shuffle.partitions')
yarnHdfs = spark.conf.get(key= 'spark.yarn.appMasterEnv.HDFS_PATH')

print(f'Number of shuffle partitions {noPartitions}')
print(f'Yarn HDFS path {yarnHdfs}')

