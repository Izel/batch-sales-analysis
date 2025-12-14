from pyspark.sql import SparkSession

PROJECT_ID = "sales-batch-data-analysis"
REGION = "europe-west2"

CONFIG_BUCKET = f"gs://{PROJECT_ID}-config"
STAGING_BUCKET = f"gs://{PROJECT_ID}-staging"

# Define GCS paths for the Iceberg and BigQuery jars
ICEBERG_SPARK_RUNTIME_PATH = f"{CONFIG_BUCKET}/iceberg/config"
ICEBERG_SPARK_RUNTIME_JAR = (
    f"{ICEBERG_SPARK_RUNTIME_PATH}/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
)
ICEBERG_BIGQUERY_CATALOG_JAR = (
    "gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar"
)

# Define catalog properties
CATALOG_BUCKET = "gs://dw-387711160261-bucket"  # f"gs://{PROJECT_ID}-catalog"
CATALOG_NAME = "bqms"
CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"
CATALOG_BIGQUERY_IMPL = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"
WAREHOUSE_PATH = f"{CATALOG_BUCKET}/warehouse"

# Input and output table names
INPUT_ORDERS_TABLE = f"{CATALOG_NAME}.ecommerce.orders"
INPUT_ORDER_ITEMS_TABLE = f"{CATALOG_NAME}.ecommerce.order_items"
OUTPUT_TABLE = f"{CATALOG_NAME}.ecommerce.top_10_items_per_city_recent"


# SparkSession Initialization with Iceberg and BigQuery Catalog
spark = (
    SparkSession.builder.appName("Top20ItemsPerCityLastHourSQL")
    .config("spark.jars", f"{ICEBERG_SPARK_RUNTIME_JAR},{ICEBERG_BIGQUERY_CATALOG_JAR}")
    .config(f"spark.sql.catalog.{CATALOG_NAME}", CATALOG_IMPL)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", CATALOG_BIGQUERY_IMPL)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.gcp_project", PROJECT_ID)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.gcp_location", REGION)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")

spark.sql("DROP VIEW IF EXISTS item_sales_per_city_view")
spark.sql("DROP VIEW IF EXISTS joined_recent_orders_items_view")
spark.sql("DROP VIEW IF EXISTS orders_items_view")
spark.sql("DROP VIEW IF EXISTS orders_view")

# Register Base Tables as Temporary Views for Spark SQL
spark.sql(f"""
      CREATE TEMPORARY VIEW orders_view AS
      SELECT 
      o.ORDER_ID, o.DELIVERY_CITY, o.CREATED_AT, O.ITEM_ID, O.QUANTITY 
      FROM {INPUT_ORDERS_TABLE}
""")

spark.sql(f"""
      CREATE TEMPORARY VIEW order_items_view AS
      SELECT order_id, ID AS ITEM_ID, 1 AS QUANTITY
      FROM {INPUT_ORDER_ITEMS_TABLE}
      GROUP BY delivery_city, item_id
""")

# Step 1: Join orders_view and order_items_view and filter for recent orders
spark.sql("""
      CREATE TEMPORARY VIEW joined_recent_orders_items_view AS 
      SELECT o.ORDER_ID, o.DELIVERY_CITY, o.CREATED_AT, oi.ITEM_ID, oi.QUANTITY
      FROM orders_view o
      JOIN order_items_view oi
      ON o.ORDER_ID = oi.ORDER_ID
      WHERE o.CREATED_AT >= (TIMESTAMP '2005-09-23 10:55:00' - INTERVAL 1 HOUR)
""")

# Step 2: Aggregate item sales per city
spark.sql("""
      CREATE TEMPORARY VIEW item_sales_per_city_view AS 
      SELECT DELIVERY_CITY, ITEM_ID, SUM(QUANTITY) AS total_quantity_sold
      FROM joined_recent_orders_items_view
      GROUP BY DELIVERY_CITY, ITEM_ID
""")

# Step 3: Rank items within each city
spark.sql("""
      CREATE TEMPORARY VIEW ranked_items_view AS 
      SELECT DELIVERY_CITY, ITEM_ID, total_quantity_sold,
      RANK() OVER (PARTITION BY DELIVERY_CITY ORDER BY total_quantity_sold DESC) AS rank
      FROM item_sales_per_city_view
""")

# Step 4: Filter for top 20 ranked items and get final DataFrame
top_items_per_city_df = spark.sql("""
      SELECT DELIVERY_CITY, ITEM_ID, TOTAL_QUANTITY_SOLD 
      FROM ranked_items_view 
      WHERE rank <= 10
""")

# Write Results to a New Iceberg Table
top_items_per_city_df.writeTo(f"{OUTPUT_TABLE}").using("iceberg").createOrReplace()

# Stop the SparkSession (essential for standalone scripts)
spark.stop()
