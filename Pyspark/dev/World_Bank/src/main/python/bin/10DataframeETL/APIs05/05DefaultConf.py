from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('Repartition')\
        .master('local[*]')\
        .getOrCreate()


print(spark.conf.get("spark.sql.files.maxPartitionBytes"))

spark.conf.set("spark.sql.files.maxPartitionBytes" , 5*1024*1024)

print(spark.conf.get("spark.sql.files.maxPartitionBytes"))

'''
hdfs dfs -stat %o /practice/test
33 files
part-00000.csv  ~5.0 MB
part-00001.csv  ~5.4 MB
...
part-00031.csv  ~5.7 MB



Total loaded data ≈ 175 MB
spark.sql.files.maxPartitionBytes = 128 MB

175 MB / 128 MB ≈ 1.3
→ rounded + parallelism heuristics
→ 4 Spark partitions

'''


spark.stop()

