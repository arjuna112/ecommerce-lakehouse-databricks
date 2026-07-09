import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

catalog_name = 'ecommerce'

df = spark.read.table(f"{catalog_name}.silver.slv_order_items")

# add gross amount column
df = df.withColumn('gross_amount', F.col('quantity') * F.col('unit_price'))

# add discount_amount column (discount_pct is already numeric, e.g. 22 -> 22%)
df = df.withColumn("discount_amount",
                    F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0))
                    )

# add net sale amount column
df = df.withColumn("sale_amount", F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))

# add date_id surrogate key
df = df.withColumn('date_id', F.date_format(F.col('dt'), 'yyyyMMdd').cast("Int"))

# add coupon flag: 1 if coupon_code is present, else 0
df = df.withColumn('coupon_flag', F.when(F.col('coupon_code').isNotNull(), 1).otherwise(0))

# fixed FX rates for currency conversion to INR
fx_rates = {
    "INR": 1.00,
    "AED": 24.18,
    "AUD": 57.55,
    "CAD": 62.93,
    "GBP": 117.98,
    "SGD": 68.18,
    "USD": 88.29,
}
rates = [(k, float(v)) for k, v in fx_rates.items()]

rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])

df = (df
      .join(
          rates_df,
          F.upper(F.trim(F.col("unit_price_currency"))) == rates_df.currency,
          "left"
      )
      .withColumn("sale_amount_inr", F.col("sale_amount") * F.col("inr_rate"))
      .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount_inr")))
      )

orders_gold_df = df.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr")
)

orders_gold_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_fact_order_items")
