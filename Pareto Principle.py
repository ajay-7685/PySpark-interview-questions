from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("ParetoSalesAnalysis").getOrCreate()

# Read the superstore dataset (adjust path if needed)
# Example: CSV file
superstore_df = spark.read.csv("superstore.csv", header=True, inferSchema=True)

# 1. order_wise_sales: Aggregate sales per order_id
order_wise_sales = (
    superstore_df
    .groupBy("order_id")
    .agg(F.sum("sales").alias("product_sales"))
)

# 2. Window specs
running_total_window = Window.orderBy(F.col("product_sales").desc()) \
                             .rowsBetween(Window.unboundedPreceding, Window.currentRow)

id_window = Window.orderBy("order_id")

total_sales_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# 3. Calculate running total, total sales threshold, and row number
cal_sales = (
    order_wise_sales
    .withColumn("running_total", F.sum("product_sales").over(running_total_window))
    .withColumn("total_sales", F.lit(0.8) * F.sum("product_sales").over(total_sales_window))
    .withColumn("id", F.row_number().over(id_window))
)

# 4. Filter for orders contributing to <= 80% of total sales
pareto_orders = cal_sales.filter(F.col("running_total") <= F.col("total_sales"))

# Show result
pareto_orders.show()
