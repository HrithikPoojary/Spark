from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder\
	.appName('Load')\
	.master('local[2]')\
	.getOrCreate()


df = spark.read.load(path ='/practice/retail_db/orders' , format = 'csv' , sep = ',' ,
			schema = ('orderid int , order_dt timestamp , cus_order_id int ,order_status string'))


print('Normal')
ordGroupByStatus = df.groupBy(df.order_status).count()
ordGroupByStatus.show()


print('\nUsing Agg function')
df.groupBy(df.order_status).agg(count(df.order_status).alias("ccount")).show()


print('\nNumber Partitions')

#consider we have hundred partitions
orderGroupByStatus = ordGroupByStatus.repartition(10)
print(orderGroupByStatus.rdd.getNumPartitions())

#each partition one file would be created
#requirement to create only 2 files but we have 10 partition

orderGroupByStatus.coalesce(2).write.save(path = '/practice/dump/retail_db/ordgroupbystatus'
					  ,format = 'csv'
					   , sep = '#'
					    ,compression = 'snappy')
# if i run this again it will throw error because mode ('error')  file is already exist we can use mode = ('overwrite' or 'append' , 'ignore(does nothing)')

orderGroupByStatus.coalesce(2).write.save(path = '/practice/dump/retail_db/ordgroupbystatus',
					  format = 'csv',
					  mode = 'overwrite',
					  sep = '$')

orderGroupByStatus.coalesce(2).write.save(path = '/practice/dump/retail_db/ordgroupbystatus',
                                          format = 'csv',
                                          mode = 'overwrite',
                                          sep = '$',
                                          header = True)

spark.stop()
