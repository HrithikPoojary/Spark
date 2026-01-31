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


#saveAsTable(name , format , mode,compression , partitionBy)
#if table is exists we have to use mode

df.write.saveAsTable('default.orders_tbl' , mode = 'overwrite' , format = 'orc')


spark.stop()
