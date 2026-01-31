from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

spark = SparkSession.builder\
        .appName('Load')\
        .master('local[2]')\
        .getOrCreate()


df = spark.read.load(path ='/practice/retail_db/orders' , format = 'csv' , sep = ',' ,
                        schema = ('orderid int , order_dt timestamp , cus_order_id int ,order_status string'))

#by default no compression is there
df.coalesce(1).write.save(path = '/practice/dump/retail_db/ordjson',format = 'json' , compression = 'bzip2', mode = 'overwrite')


spark.stop()
