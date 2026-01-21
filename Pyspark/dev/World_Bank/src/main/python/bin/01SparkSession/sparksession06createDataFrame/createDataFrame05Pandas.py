from pyspark.sql import SparkSession   #type:ignore
import pandas as pd    #type:ignore

spark = SparkSession.builder\
	.appName('Create Dataframe')\
	.master('local[*]')\
	.getOrCreate()

lst = [['tom',10],['nick',15],['juli',14]]
df_pandas = pd.DataFrame(data = lst , columns = ['Name','Age'])

print(df_pandas)

df = spark.createDataFrame(data = df_pandas)

df.show()
