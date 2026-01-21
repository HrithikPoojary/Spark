from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

rdd = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

julyOdd = rdd.filter(lambda x : x.split(',')[1].split('-')[1]== '07')

augOdd = rdd.filter(lambda x : x.split(',')[1].split('-')[1]== '08')

rddJulAug = julyOdd.union(augOdd).distinct()

print(f'Partition created for jul and Aug union data {rddJulAug.getNumPartitions()}')

# Requirement to create 5 files

rddJulAug = rddJulAug.repartition(5)

# As of now i have no compression file available in core-site.xml
rddJulAug.saveAsTextFile('/practice/dump/')

#After this files will be created
# -rw-r--r--   1 hrithik_poojary supergroup          0 2026-01-11 16:48 /practice/dump/_SUCCESS
# -rw-r--r--   1 hrithik_poojary supergroup       7823 2026-01-11 16:48 /practice/dump/part-00000
# -rw-r--r--   1 hrithik_poojary supergroup       7762 2026-01-11 16:48 /practice/dump/part-00001
# -rw-r--r--   1 hrithik_poojary supergroup       7758 2026-01-11 16:48 /practice/dump/part-00002
# -rw-r--r--   1 hrithik_poojary supergroup       7754 2026-01-11 16:48 /practice/dump/part-00003
# -rw-r--r--   1 hrithik_poojary supergroup       7870 2026-01-11 16:48 /practice/dump/part-00004
