from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as min_, when, sum as sum_

# Initialize Spark session
spark = SparkSession.builder.appName("OrdersAnalysis").getOrCreate()

# Step 1: Create data as a list of tuples
data = [
    (1, 100, '2022-01-01', 2000),
    (2, 200, '2022-01-01', 2500),
    (3, 300, '2022-01-01', 2100),
    (4, 100, '2022-01-02', 2000),
    (5, 400, '2022-01-02', 2200),
    (6, 500, '2022-01-02', 2700),
    (7, 100, '2022-01-03', 1000),
    (8, 400, '2022-01-03', 3000),
    (9, 600, '2022-01-03', 3000)
]

# Step 2: Define schema column names
columns = ["order_id", "customer_id", "order_date", "order_amount"]

# Step 3: Create DataFrame
orders = spark.createDataFrame(data, schema=columns)

# Step 4: Find each customer's first order date
first_order = (
    orders.groupBy("customer_id")
    .agg(min_("order_date").alias("first_order_date"))
)

# Step 5: Join orders with first_order to tag each order
visited = (
    orders.join(first_order, on="customer_id", how="inner")
    .withColumn(
        "new_customer",
        when(col("order_date") == col("first_order_date"), 1).otherwise(0)
    )
    .withColumn(
        "repeated_customer",
        when(col("order_date") != col("first_order_date"), 1).otherwise(0)
    )
)

# Step 6: Aggregate by date to get new and repeated customer counts
result = (
    visited.groupBy("order_date")
    .agg(
        sum_("new_customer").alias("ncc"),
        sum_("repeated_customer").alias("rpc")
    )
    .orderBy("order_date")
)

# Show result
result.show()
