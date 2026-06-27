# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold: Building BI Ready Tables

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType ,FloatType
from pyspark.sql import Row
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC # Products

# COMMAND ----------

# Loading all 3 tables into spark DF
df_products = spark.read.table(f'{catalog_name}.silver.slv_products')
df_brands = spark.read.table(f'{catalog_name}.silver.slv_brands')
df_category = spark.read.table(f'{catalog_name}.silver.slv_category')

# COMMAND ----------

# Create 3 temporary views for the 3 tables
df_products.createOrReplaceTempView('v_products')
df_brands.createOrReplaceTempView('v_brands')
df_category.createOrReplaceTempView('v_category')

# COMMAND ----------

display(spark.sql("select * from v_products limit 10"))


# COMMAND ----------

display(spark.sql("select * from v_brands limit 5"))


# COMMAND ----------

display(spark.sql("select * from v_category limit 5"))


# COMMAND ----------

# Make sure to use the correct catalog name called ecommerce
spark.sql(f"USE CATALOG {catalog_name}")



# COMMAND ----------

# DBTITLE 1,Cell 11
# MAGIC %sql
# MAGIC
# MAGIC -- Create New table called gld_dim_products in gold layer
# MAGIC -- Build  brandsxcategory mapping and write to gold layer
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC
# MAGIC WITH brands_categories AS(
# MAGIC     SELECT 
# MAGIC     b.brand_name,
# MAGIC     b.brand_code,
# MAGIC     c.category_name,
# MAGIC     c.category_code
# MAGIC     FROM v_brands b
# MAGIC     INNER JOIN v_category c
# MAGIC     ON b.category_code = c.category_code
# MAGIC )
# MAGIC select 
# MAGIC   p.product_id,
# MAGIC   p.sku,
# MAGIC   p.category_code,
# MAGIC   coalesce(bc.category_name, 'Not Available') AS category_name,
# MAGIC   p.brand_code,
# MAGIC   coalesce(bc.brand_name, 'Not Available') AS brand_name,
# MAGIC   p.color,
# MAGIC   p.size,
# MAGIC   p.material,
# MAGIC   p.weight_grams,
# MAGIC   p.length_cm,
# MAGIC   p.width_cm,
# MAGIC   p.height_cm,
# MAGIC   p.rating_count,
# MAGIC   p.file_name,
# MAGIC   p.ingest_timestamp
# MAGIC from v_products p
# MAGIC LEFT join brands_categories bc
# MAGIC  ON p.brand_code = bc.brand_code;

# COMMAND ----------

# MAGIC %md
# MAGIC # Customers

# COMMAND ----------

# For customers table it will be very useful to have region
# India state
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}

# Australia states
australia_region = {
    "VIC": "South_east", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

#United states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}

# COMMAND ----------

country_state_map

# COMMAND ----------

# 1. Flatten country_state_map into list of rows

rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(COUNTRY=country, STATE=state_code, region=region))
rows[:20]           

# COMMAND ----------

# 2. Create Mapping DataFrame
df_region_mapping = spark.createDataFrame(rows)

df_region_mapping.show(truncate=False)

# COMMAND ----------

# Now ill do the join 
# Lets create a Silver table first
df_silver = spark.read.table(f"{catalog_name}.silver.slv_customers")
display(df_silver.limit(10))


# COMMAND ----------

#Performing The Join
df_gold = df_silver.join(df_region_mapping, on=['country','state'], how="left")

display(df_gold.limit(5))

# Check if region column still has any null values
display(df_gold.filter(df_gold.region.isNull()).limit(10))


# COMMAND ----------

# since there are some null values, I will replace with other
df_gold = df_gold.fillna('Other', subset=['REGION'])
display(df_gold.filter(df_gold.region.isNull()).limit(10))
display(df_gold.limit(5))


# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)
df_gold.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #Date/Calendar

# COMMAND ----------

df_silver = spark.read.table(f"{catalog_name}.silver.slv_calendar")
display(df_silver.limit(10))

# COMMAND ----------

# Adde date_id column
df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

#Add Month Name (ex:- 'January', 'Febrauary', etc)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

# Add is_weekend column
df_gold = df_gold.withColumn("is_weekend", F.when(F.col("day_name").isin("Saturday","Sunday"), 1).otherwise(0))

display(df_gold.limit(5))

# COMMAND ----------

# Reorder certain columns 
desired_columns_order = ["date_id","date","year","month_name","day_name","is_weekend","quarter","week","ingested_at","_Source_file"]

df_gold = df_gold.select(desired_columns_order)
display(df_gold.limit(5))

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_date)
df_gold.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED ecommerce.gold.gld_dim_date;
