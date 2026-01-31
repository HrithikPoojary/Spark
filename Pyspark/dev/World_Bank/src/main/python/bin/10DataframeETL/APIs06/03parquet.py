from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

spark = SparkSession.builder\
        .appName('Load')\
        .master('local[2]')\
        .getOrCreate()


df = spark.read.load(path ='/practice/retail_db/orders' , format = 'csv' , sep = ',' ,
                        schema = ('orderid int , order_dt timestamp , cus_order_id int ,order_status string'))



#print(help(df.write.parquet))

#default compression type
#orc is also same parameter like parquet
#spark.sql.orc.comppresion.codec


print(spark.conf.get("spark.sql.parquet.compression.codec"))

df.write.save('/practice/dump/retail_db/ordpaquet' , format = 'parquet' , partitionBy = 'order_status' , mode = 'overwrite')

spark.stop()
