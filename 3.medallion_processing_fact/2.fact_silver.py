import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

catalog_name = 'ecommerce'

df = spark.read.table(f"{catalog_name}.bronze.brz_order_items")

# drop duplicate line items
df = df.dropDuplicates(["order_id", "item_seq"])

# convert 'Two' to 2 and cast quantity to Integer
df = df.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2)
     .otherwise(F.col("quantity").cast(IntegerType()))
)

# remove '$' from unit_price and cast to Double
df = df.withColumn("unit_price", F.regexp_replace("unit_price", "[$]", "").cast("double"))

# remove '%' from discount_pct and cast to Double
df = df.withColumn("discount_pct", F.regexp_replace("discount_pct", "%", "").cast("double"))

# normalize coupon_code to lowercase
df = df.withColumn("coupon_code", F.lower("coupon_code"))

# normalize channel values
df = df.withColumn("channel",
    F.when(F.col("channel") == "web", "Website")
     .when(F.col("channel") == "app", "Mobile")
     .otherwise(F.col("channel"))
)

# convert dt column (string -> date)
df = df.withColumn("dt", F.to_date("dt", "yyyy-MM-dd"))

# convert order_ts column (string -> timestamp), handling two possible source formats
df = df.withColumn(
    "order_ts",
    F.coalesce(
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),  # matches 2025-08-01 22:53:52
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")       # fallback for 01-08-2025 22:53
    )
)

# convert item_seq column (string -> integer)
df = df.withColumn("item_seq", F.col("item_seq").cast("int"))

# convert tax_amount (string -> double), stripping any non-numeric characters
df = df.withColumn("tax_amount", F.regexp_replace("tax_amount", r"[^0-9.-]", "").cast("double"))

# add processed time column
df = df.withColumn("processed_time", F.current_timestamp())

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_order_items")
