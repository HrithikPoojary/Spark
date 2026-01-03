from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder \
    .appName("CustomerDataProcessingHrithik") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.ui.enabled", "true") \
    .getOrCreate()

df = spark.createDataFrame(
    [
        (1, "Alice", "HR", 50000),
        (2, "Bob", "IT", 70000),
        (3, "Charlie", "Finance", 65000),
        (4, "Diana", "IT", 72000),
    ],
    ["id", "name", "department", "salary"]
)

df.show()

sc = spark.sparkContext
print("Spark UI:", sc.uiWebUrl)

input("Press ENTER to stop Spark...")
spark.stop()
