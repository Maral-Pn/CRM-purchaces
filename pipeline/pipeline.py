
from pyspark.sql import SparkSession
import pyspark as ps
def writer(spark : SparkSession, table_name: str, file_name:str):
    df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost/retail_db") \
        .option("dbtable", table_name) \
        .option("user", "mysql_user") \
        .option("password", "password") \
        .load()

    df.show()
    df.write.option("inferSchema", True).option("header", True).parquet( "../data_lake/bronze_layer/" + file_name)

if __name__ == "__main__":

    spark = (SparkSession
             .builder
             .config("spark.jars", "/home/hossein/Downloads/mysql-connector-j_9.0.0-1ubuntu24.04_all/usr/share/java/mysql-connector-j-9.0.0.jar")
             .appName("loader").master("local[*]").getOrCreate())

    writer(spark, "customer", "customer.parquet")
    writer(spark, "seller", "sellers.parquet")
    writer(spark, "order_item", "order_item.parquet")
    writer(spark, "product", "products.parquet")
    writer(spark, "orders", "orders.parquet")
