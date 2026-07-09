# ecommerce-lakehouse-databricks
An end-to-end data engineering project built on Databricks, implementing the Medallion Architecture (Bronze → Silver → Gold) to process ecommerce data and prepare for analytics.

**Technologies Used**
* Databricks Free Edition
* Apache Spark (PySpark)
* Delta Lake
* Python
* SQL
* Git & GitHub

**Project Workflow**
* Ingest raw data into the Bronze layer
* Clean and transform data in the Silver layer
* Create business-ready datasets in the Gold layer
* Query data using PySpark and SQL

**Project Structure**
```text
ecommerce-lakehouse-databricks/
├── 1.Setup/
├── 2.medallion_processing_dim/
├── 3.medallion_processing_fact/
├── datasets/
└── README.md
```
