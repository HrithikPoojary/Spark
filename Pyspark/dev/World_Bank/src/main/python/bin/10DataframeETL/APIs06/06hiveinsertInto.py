from pyspark.sql import SparkSession


spark = SparkSession.builder\
        .appName('Load')\
        .master('local[2]')\
	.config('spark.sql.warehouse.dir' , 'hdfs:///user/hive/warehouse')\
	.enableHiveSupport()\
        .getOrCreate()


df = spark.read \
    .format("csv") \
    .option("sep", ",") \
    .schema("""
        orderid INT,
        order_dt TIMESTAMP,
        cus_order_id INT,
        order_status STRING
    """) \
    .load("/practice/retail_db/orders")


print(spark.conf.get("spark.sql.catalogImplementation"))


spark.sql("""
CREATE TABLE IF NOT EXISTS default.orders_tbl (
    orderid INT,
    order_dt TIMESTAMP,
    cus_order_id INT,
    order_status STRING
)
STORED AS PARQUET
""")



df.write.insertInto('default.orders_tbl')

#query the table we can use >spark-sql
#desc formatted order_tbl

#if any error persisted 
#then remove all derby ,metastore warehouse related files
#rm -rf metastore_db
#rm -rf spark-warehouse
#sudo rm -rf metastore_db spark-warehouse
spark.stop()
