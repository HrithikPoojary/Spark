from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

spark = SparkSession.builder\
        .appName('Load')\
        .master('local[2]')\
        .getOrCreate()


df = spark.read.load(path ='/practice/retail_db/orders' , format = 'csv' , sep = ',' ,
                        schema = ('orderid int , order_dt timestamp , cus_order_id int ,order_status string'))

#for text first we have to convert df to text , we have to concat 4 column to one


ordtext = df.select(concat_ws('\n' , df.orderid , df.order_dt , df.cus_order_id , df.order_status).alias('concatText'))

ordtext.show(5)
ordtext = ordtext.repartition(10)

#repartition  mainly used to process the distributed way
#coalsce is used to store the data


ordtext.coalesce(5).write.save(path = '/practice/dump/retail_db/ordtext' , format = 'text')


spark.stop()








