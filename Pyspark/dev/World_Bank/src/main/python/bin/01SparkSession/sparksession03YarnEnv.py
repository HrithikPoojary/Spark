from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .master('yarn') \
        .appName('Yarn Evn')\
        .getOrCreate()

print(f"Spark Is created at {spark}")

print(f"Number of partititions {spark.conf.get('spark.sql.shuffle.partitions')}")

print(f" Yarn Path {spark.conf.get('spark.yarn.appMasterEnv.HDFS_PATH')}")