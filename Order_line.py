# Databricks notebook source


# COMMAND ----------

order_df= spark.read.format("csv").option("header","true").load("dbfs:/mnt/landing_zone/order_line.csv")
display(order_df)

# COMMAND ----------

order_df_clean = order_df.filter(order_df["quantity"].isNotNull() & order_df["price"].isNotNull() & order_df["product"].isNotNull())
display(order_df_clean)

# COMMAND ----------

from pyspark.sql.functions import col



# COMMAND ----------

# Check for blank and null values in all columns of order_df_clean and display if found any
for column in order_df_clean.columns:
    df_blank_null = order_df_clean.filter((col(column) == "") | col(column).isNull())
    if df_blank_null.count() > 0:
        display(df_blank_null)

# COMMAND ----------

duplicates = order_df.groupBy("order_id", "product").count().filter("count > 1")
display(duplicates)

# COMMAND ----------

order_df = order_df.dropDuplicates(["order_id", "product"])
display(order_df)

# COMMAND ----------

duplicates = order_df.groupBy("order_id", "product").count().filter("count > 1")
display(duplicates)

# COMMAND ----------

output_path = "/mnt/landing_zone/order_data_cleansed.csv"
order_df_clean.write.format("csv").option("header", "true").mode("overwrite").save(output_path)

# COMMAND ----------

output_path_csv="/mnt//Squad_4/gold/order_data_cleansed.csv"
order_df_clean.write.format ("csv").option("header", "true").mode("overwrite").save(output_path_csv)
