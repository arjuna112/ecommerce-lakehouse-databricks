from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
import pyspark.sql.functions as F

catalog_name = 'ecommerce'

# Brands
df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')

df_silver = df_bronze.withColumn("brand_name", F.trim(df_bronze.brand_name))

df_silver = df_silver.withColumn("brand_code", F.regexp_replace(df_silver.brand_code, r'[^a-zA-Z0-9]', ''))

anomalies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}

df_silver = df_silver.replace(anomalies, subset="category_code")

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_brands")


# Category
df_bronze = spark.table(f"{catalog_name}.bronze.brz_category").select("category_code", "category_name", "ingested_at", "_Source_file")

df_silver = df_bronze.dropDuplicates(["category_code"])

df_silver = df_silver.withColumn("category_code", F.upper(df_silver["category_code"]))

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_category")


# Products
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products").select(
    "product_id", "sku", "category_code", "brand_code", "color", "size",
    "material", "weight_grams", "length_cm", "width_cm", "height_cm",
    "rating_count", "file_name", "ingest_timestamp"
)

# weight_grams contains trailing 'g', strip it and cast to int
df_silver = df_bronze.withColumn("weight_grams", F.regexp_replace("weight_grams", "g", "").cast(IntegerType()))

# length_cm uses comma as decimal separator, replace with '.' and cast to float
df_silver = df_silver.withColumn("length_cm", F.regexp_replace("length_cm", ",", ".").cast(FloatType()))

# normalize category_code and brand_code to upper case
df_silver = df_silver.withColumn("category_code", F.upper("category_code")) \
    .withColumn("brand_code", F.upper("brand_code"))

# fix spelling mistakes in material column
df_silver = df_silver.withColumn("material", F.when(F.col("material") == "Coton", "Cotton")
                                  .when(F.col("material") == "Ruber", "Rubber")
                                  .when(F.col("material") == "Alumium", "Aluminium")
                                  .otherwise(F.col("material")))

# convert negative rating_count to positive, null -> 0
df_silver = df_silver.withColumn("rating_count", F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
                                  .otherwise(F.lit(0)))

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")


# Customers
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# drop rows with null customer_id (small volume, approved by business to drop)
df_silver = df_bronze.dropna(subset=["customer_id"])

# fill null phone values with 'Not Available'
df_silver = df_silver.fillna("Not Available", subset=["phone"])

# strip trailing '.0' from phone numbers
df_silver = df_silver.withColumn("phone", F.regexp_replace(F.col("phone"), "\\.0$", ""))

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")


# Calendar/Date
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar").select(
    "date", "year", "day_name", "quarter", "week_of_year", "_Source_file", "ingested_at"
)

df_silver = df_bronze.dropDuplicates(["date"])

# normalize casing of day_name
df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))

# convert negative week_of_year to positive
df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))

# enhance quarter and week_of_year with year suffix
df_silver = df_silver.withColumn("quarter", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year")))
df_silver = df_silver.withColumn("week_of_year", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year")))

df_silver = df_silver.withColumnRenamed("week_of_year", "week")

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar") 
