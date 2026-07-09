spark.sql("CREATE CATALOG IF NOT EXISTS ecommerce")
spark.sql("USE CATALOG ecommerce")

# Create 3 schemas following Medallion Architecture
spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.gold")

# If the catalog needs to be dropped for some reason, use below command
# spark.sql("DROP CATALOG IF EXISTS ecommerce CASCADE")
