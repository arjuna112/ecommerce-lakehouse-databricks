from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
import pyspark.sql.functions as F

catalog_name = 'ecommerce'

# Brands
brand_schema = StructType([
    StructField('brand_code', StringType(), False),
    StructField('brand_name', StringType(), True),
    StructField('category_code', StringType(), True),
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/*.csv"

df = spark.read \
    .option('header', 'true') \
    .option('delimiter', ',') \
    .schema(brand_schema) \
    .csv(raw_data_path)

df = df.withColumn('_Source_file', F.col('_metadata.file_path')) \
    .withColumn('ingested_at', F.current_timestamp())

df.write.format('delta') \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_brands")


# Category
category_schema = StructType([
    StructField('category_code', StringType(), False),
    StructField('category_name', StringType(), True),
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/category/*.csv"

df_raw = spark.read \
    .option('header', 'true') \
    .option('delimiter', ',') \
    .schema(category_schema) \
    .csv(raw_data_path)

df_raw = df_raw.withColumn('_Source_file', F.col('_metadata.file_path')) \
    .withColumn('ingested_at', F.current_timestamp())

df_raw.write.format('delta') \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_category")


# Products
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),
    StructField("length_cm", StringType(), True),
    StructField("width_cm", StringType(), True),
    StructField("height_cm", StringType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("file_name", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False)
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/products/*.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(products_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp())

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_products")


# Customers
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/customers/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp())

df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_customers")


# Date
date_schema = StructType([
    StructField('date', DateType(), False),
    StructField('year', IntegerType(), True),
    StructField('day_name', StringType(), True),
    StructField('quarter', IntegerType(), True),
    StructField('week_of_year', IntegerType(), True),
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/date/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").option("dateFormat", "d-M-yyyy").schema(date_schema).csv(raw_data_path) \
    .withColumn("_Source_file", F.col("_metadata.file_path")) \
    .withColumn("ingested_at", F.current_timestamp())

df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_calendar")
