# ecommerce-lakehouse-databricks
An end-to-end data engineering project built on Databricks, implementing the Medallion Architecture (Bronze → Silver → Gold) to process ecommerce data and prepare for analytics.

<img width="666" height="256" alt="image" src="https://github.com/user-attachments/assets/52422c73-8e39-4cdf-8fd6-b4537a5e7381" />


**Technologies Used**
* Databricks
* Apache Spark (PySpark)
* Delta Lake
* Python
* SQL
* GitHub

**Project Workflow**
* Ingest raw data into the Bronze layer
* Clean and transform data in the Silver layer
* Create business-ready datasets in the Gold layer
* Query data using PySpark and SQL

**Dashboard & Insights**
* Dashboard is built directly on top of Gold-layer tables (gld_dim_products, fact tables) generated in 3.medallion_processing_fact/
<img width="1312" height="690" alt="image" src="https://github.com/user-attachments/assets/201acc6c-cbd4-4221-b658-0626b352502a" />

**Key insights:**
* Monthly sales trend showing a dip in Sep 2025 (~575M) before recovering in Oct 2025 (~618M)
* Electronics is the dominant category by net sales, far ahead of Home & Kitchen and Apparel
* Heatmap of order volume by day/hour reveals peak activity clusters (e.g., early morning hours on weekends) — useful for staffing/inventory planning

**Project Structure**
```text
ecommerce-lakehouse-databricks/
├── 1.Setup/
├── 2.medallion_processing_dim/
├── 3.medallion_processing_fact/
├── datasets/
├── images/
└── README.md
```
