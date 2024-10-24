# Databricks notebook source
customer_behaviour_df= spark.read.format("csv").option("header","true").load("dbfs:/mnt/landing_zone/,customer_behaviour.csv")
display(customer_behaviour_df)

# COMMAND ----------

customer_behaviour_df_clean = customer_behaviour_df.filter(
    customer_behaviour_df["customer_id"].isNotNull() &
    customer_behaviour_df["order_frequency"].isNotNull() &
    customer_behaviour_df["average_order_value"].isNotNull() &
    customer_behaviour_df["customer_lifetime_value"].isNotNull() &
    customer_behaviour_df["website_visit"].isNotNull() &
    customer_behaviour_df["seconds_spend_on_website"].isNotNull() &
    customer_behaviour_df["page_views"].isNotNull() &
    customer_behaviour_df["cart_abandonment_rate"].isNotNull())

# COMMAND ----------

display(customer_behaviour_df_clean)
