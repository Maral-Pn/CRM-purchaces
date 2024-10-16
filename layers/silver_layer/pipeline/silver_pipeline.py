from dataclasses import replace
from datetime import datetime
from itertools import count

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum



def drop_duplicate(df):
    duplicate_rows = df.count() - df.dropDuplicates().count()
    print(f"Number of duplicate rows: {duplicate_rows}")
    dropped_df = df.dropDuplicates()
    return dropped_df


def unique_id(df):
    print(
        f"ID column of dataframe has :"
        f" {df.select("ID").distinct().count()} Distinct values")
    duplicated_id_df = df.groupby(col("ID")).count().filter("count > 1")
    df_result = df.join(duplicated_id_df, on="ID", how="left_anti")
    return df_result


def null_value(df):

    result_df = df.fillna(0).fillna("")
    return result_df


def apply_data_quality_rules(spark_session: SparkSession, table_name: str):
    df = spark_session.read.parquet("../../../data_lake/bronze_layer/" + table_name)
    df0 = drop_duplicate(df)
    df1 = unique_id(df0)
    df2 = null_value(df1)
    return df2


if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .config("spark.jars",
                "/home/hossein/Downloads/mysql-connector-j_9.0.0-1ubuntu24.04_all/usr/share/java/mysql-connector-j-9.0.0.jar")
        .appName("loader")
        .master("local[*]")
        .getOrCreate()
         )
    curated_customer_df = apply_data_quality_rules(spark, "customer.parquet")
    curated_order_item_df = apply_data_quality_rules(spark, "order_item.parquet")
    curated_orders_df = apply_data_quality_rules(spark, "orders.parquet")
    curated_products_df = apply_data_quality_rules(spark, "products.parquet")
    curated_sellers_df = apply_data_quality_rules(spark, "sellers.parquet")

    curated_customer_df.show()
    curated_order_item_df.show()
    curated_orders_df.show()
    curated_products_df.show()
    curated_sellers_df.show()
