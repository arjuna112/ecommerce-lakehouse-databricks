from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row
import pyspark.sql.functions as F

catalog_name = 'ecommerce'

# Products
df_products = spark.read.table(f'{catalog_name}.silver.slv_products')
df_brands = spark.read.table(f'{catalog_name}.silver.slv_brands')
df_category = spark.read.table(f'{catalog_name}.silver.slv_category')

df_products.createOrReplaceTempView('v_products')
df_brands.createOrReplaceTempView('v_brands')
df_category.createOrReplaceTempView('v_category')

spark.sql(f"USE CATALOG {catalog_name}")

# Build brand x category mapping and join onto products to create gld_dim_products
spark.sql("""
CREATE OR REPLACE TABLE gold.gld_dim_products AS

WITH brands_categories AS (
    SELECT
        b.brand_name,
        b.brand_code,
        c.category_name,
        c.category_code
    FROM v_brands b
    INNER JOIN v_category c
        ON b.category_code = c.category_code
)
SELECT
    p.product_id,
    p.sku,
    p.category_code,
    coalesce(bc.category_name, 'Not Available') AS category_name,
    p.brand_code,
    coalesce(bc.brand_name, 'Not Available') AS brand_name,
    p.color,
    p.size,
    p.material,
    p.weight_grams,
    p.length_cm,
    p.width_cm,
    p.height_cm,
    p.rating_count,
    p.file_name,
    p.ingest_timestamp
FROM v_products p
LEFT JOIN brands_categories bc
    ON p.brand_code = bc.brand_code
""")


# Customers
# region mapping per country/state, used to enrich customer records with a region

india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}

australia_region = {
    "VIC": "South_east", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West",
    "NY": "NorthEast", "TX": "South"
}

uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

singapore_region = {
    "SG": "Singapore"
}

canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}

# flatten country_state_map into a mapping DataFrame
rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(COUNTRY=country, STATE=state_code, region=region))

df_region_mapping = spark.createDataFrame(rows)

df_silver = spark.read.table(f"{catalog_name}.silver.slv_customers")

df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how="left")

# some customers don't match the mapping, default their region to 'Other'
df_gold = df_gold.fillna('Other', subset=['REGION'])

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")


# Date/Calendar
df_silver = spark.read.table(f"{catalog_name}.silver.slv_calendar")

# surrogate date_id key in yyyyMMdd format
df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

# full month name
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

# flag weekends
df_gold = df_gold.withColumn("is_weekend", F.when(F.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0))

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "ingested_at", "_Source_file"]

df_gold = df_gold.select(desired_columns_order)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")
