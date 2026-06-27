# Databricks notebook source
# MAGIC %sql
# MAGIC Create catalog if not exists ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;

# COMMAND ----------

# MAGIC %md
# MAGIC # creating 3 schemas using Medallion Architecture

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES from ecommerce;

# COMMAND ----------

# IF I WANT TO DROP the catalog for some reasone.. use below command

# %sql
# DROP CATALOG IF EXISTS ecommerce CASCADE;
