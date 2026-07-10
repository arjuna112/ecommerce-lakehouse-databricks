# ecommerce-lakehouse-databricks
An end-to-end data engineering project built on Databricks, implementing the Medallion Architecture (Bronze → Silver → Gold) to process ecommerce data and prepare for analytics.

<img width="666" height="256" alt="image" src="https://github.com/user-attachments/assets/52422c73-8e39-4cdf-8fd6-b4537a5e7381" />


**Technologies Used**
* Databricks
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
