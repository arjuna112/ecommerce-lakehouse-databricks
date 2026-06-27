# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
import pyspark.sql.functions as F


# COMMAND ----------

catalog_name='ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC #Brands

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC # Remove the leading and trailing spaces from the "brand_name" and lets assign that to new DF called silver

# COMMAND ----------

df_silver = df_bronze.withColumn("brand_name", F.trim(df_bronze.brand_name))
df_silver.show(10)

# COMMAND ----------

# i see speacial character in brand_code.. Ex:- VOLT@
# so i am removing special character using regexp_replace.

df_silver = df_silver.withColumn("brand_code", F.regexp_replace(df_silver.brand_code, r'[^a-zA-Z0-9]', ''))
df_silver.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC # lets identify all unique category_code,brand_code and so on.

# COMMAND ----------

df_silver.select("category_code").distinct().show(10)

# COMMAND ----------

# Anomalies in category_code that i found (AND INFORMED TO BUSINESS Managers AND They can say that replcae with below.)
anomalies={
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}

df_silver = df_silver.replace(anomalies, subset="category_code")
df_silver.select("category_code").distinct().show(50)


# COMMAND ----------

# Now lets write the data to the silver schema (catalog:ecommerce, schema:silver, table:slv_brands)
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------

# COMMAND ----------

# MAGIC %md
# MAGIC # Now lets do the DATA CLEANING for other tables - 'Category, Products, Customers, Date'

# COMMAND ----------

# MAGIC %md
# MAGIC # Category

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_category").select("category_code", "category_name","ingested_at","_Source_file")

df_bronze.show(10)

# COMMAND ----------


df_duplicates = df_bronze.groupBy("category_code").count().filter("count > 1")
df_duplicates.show(10)
# i found duplicate rows based on category_code in df_bronze

# COMMAND ----------

# Removing duplicate rows based on category_code in df_bronze
df_silver = df_bronze.dropDuplicates(["category_code"])
df_silver.show()

# COMMAND ----------

df_silver = df_silver.withColumn("category_code", F.upper(df_silver["category_code"]))
df_silver.show()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC #Products

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_products)
df_bronze = spark.read.table((f"{catalog_name}.bronze.brz_products")).select("product_id","sku","category_code","brand_code","color","size","material","weight_grams","length_cm","width_cm","height_cm","rating_count","file_name","ingest_timestamp")

# get the row and column count of the bronze table (ecommerce.bronze.brz_products)
row_count = df_bronze.count()
column_count = len(df_bronze.columns)
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

# COMMAND ----------

display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC check weight_grams contains g

# COMMAND ----------

#check weight_grams column
df_bronze.select("weight_grams").show(10, truncate=False)

# COMMAND ----------

# replace 'g' with ''(string) and cast to IntergerType.
df_silver = df_bronze.withColumn("weight_grams", F.regexp_replace("weight_grams", "g", "").cast(IntegerType()))
df_silver.select("weight_grams").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC check length_cm column contains ,

# COMMAND ----------

# check length_cm column (instead of , use .)
df_silver.select("length_cm").show(10, truncate=False)

# COMMAND ----------

# replace length_cm column ',' with '.' and cast to FloatType.
df_silver = df_silver.withColumn("length_cm", F.regexp_replace("length_cm", ",", ".").cast(FloatType()))
df_silver.select("length_cm").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC "category_code" and "brand_code" are in lower case. Make it all upper case.

# COMMAND ----------

df_silver.select("category_code","brand_code").show(5)

# COMMAND ----------

# convert "category_code" and "brand_code" to upper case.
df_silver = df_silver.withColumn("category_code", F.upper("category_code"))\
    .withColumn("brand_code", F.upper("brand_code"))
df_silver.select("category_code","brand_code").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC Spelling Mistakes in "material" Column

# COMMAND ----------

df_silver.select("material").distinct().show()


# COMMAND ----------

# Correct the spelling mistakes in "material" column.
df_silver = df_silver.withColumn("material", F.when(F.col("material") == "Coton", "Cotton")
                                .when(F.col("material") == "Ruber", "Rubber")\
                                .when(F.col("material") == "Alumium", "Aluminium")\
                                .otherwise(F.col("material")))
df_silver.select("material").distinct().show()




# COMMAND ----------

# MAGIC %md
# MAGIC Negative values in "Rating_count" column

# COMMAND ----------

# find negative columns in rating_count

df_silver.filter(df_silver.rating_count < 0).select("rating_count").show(5)

# COMMAND ----------

#convert Negative rating_count to positive
df_silver = df_silver.withColumn("rating_count",F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
                                 .otherwise(F.lit(0))) # If null, Replace with 0

df_silver.filter(df_silver.rating_count < 0).select("rating_count").show(5)                                 



# COMMAND ----------

# Check final cleaned data

df_silver.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_products)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC # Customers

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_customers)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# get the row and column count of the bronze table (ecommerce.bronze.brz_customers)
row_count= df_bronze.count()
column_count = len(df_bronze.columns)
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL Values in "customer_id" column

# COMMAND ----------

#Handle Null values in customer_id column
null_count = df_bronze.filter(df_bronze.customer_id.isNull()).count()
print(f"customer_id column Null count: {null_count}")

# COMMAND ----------

# There are 300 null values in customer_id column. Display some of those
df_bronze.filter(df_bronze.customer_id.isNull()).show(5)


# COMMAND ----------

# (1. if i have too many null values.. then i can replace with some valid value. 2)But here just 300 so after discussing with Business Manager, they informed its okay to drop the data

# Drop rows where 'customer_id' is null 
df_silver = df_bronze.dropna(subset=["customer_id"])

row_count = df_silver.count()
print(f"Row count after dropping customer id null values: {row_count}")

# OR, i can also check if nulls are there.. or not by using below command
#df_silver.filter(df_silver.customer_id.isNull()).show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC Handle NULL values in "phone" column

# COMMAND ----------

null_count= df_silver.filter(df_silver.phone.isNull()).count()
print(f"phone column Null count: {null_count}")

# COMMAND ----------

# There are 29964 Null values in Phone column. Display some of those
df_silver.filter(df_silver.phone.isNull()).show(5)


# COMMAND ----------

# Fill Null values with Not Available
df_silver = df_silver.fillna("Not Available", subset=["phone"])

# check if phone column still has any nulls
df_silver.filter(df_silver.phone.isNull()).show()

# COMMAND ----------

df_silver.filter(df_silver.phone.isNotNull()).show(2)

# COMMAND ----------

# Remove .0 at the end of phone column
df_silver = df_silver.withColumn("phone", F.regexp_replace(F.col("phone"), "\\.0$", ""))

df_silver.filter(df_silver.phone.isNotNull()).show(2)

# COMMAND ----------

n = df_silver.count()
print(f"number of rows in silver table: {n}")

# COMMAND ----------

# Write the raw data to the silver table (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC # Calendar/Date

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar").select("date","year","day_name","quarter","week_of_year","_Source_file","ingested_at")

# get the row and column count of the bronze table (ecommerce.bronze.brz_calendar)
row_count= df_bronze.count()
column_count = len(df_bronze.columns)
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")


# COMMAND ----------

print(df_bronze.printSchema())

df_bronze.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Duplicates in date column

# COMMAND ----------

# check duplicate rows in date column
df_duplicates = df_bronze.groupBy("date").count().filter("count > 1")
display(df_duplicates)

# COMMAND ----------

# Remove duplicate Rows in date
df_silver = df_bronze.dropDuplicates(["date"])
rows = df_silver.count()
print(f"Rows after removing duplicate : {rows}") 

# COMMAND ----------

# MAGIC %md
# MAGIC "day_name" column normalize casing

# COMMAND ----------

# Capitalize first letter of each word in "day_name" column
df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))
df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC convert Negative "week_of_year" column to positive

# COMMAND ----------

# convert negative "week_of_year" to positive
df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))
df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Enhance "quater" and "week_of_year" column

# COMMAND ----------

# enhance "quarter" and "week_of_year" column
df_silver = df_silver.withColumn("quarter", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year")))

df_silver = df_silver.withColumn("week_of_year", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year")))

df_silver.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns

# COMMAND ----------

# Rename "week" column
df_silver = df_silver.withColumnRenamed("week_of_year", "week")

df_silver.show(5)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")
