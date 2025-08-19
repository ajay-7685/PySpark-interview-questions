✅ Setup sample data in PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, when
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("SecondItemBrandCheck").getOrCreate()

# Sample data for orders
orders_data = [
    (1, "2019-08-01", 1, 1, 3),
    (2, "2019-08-02", 2, 3, 1),
    (3, "2019-08-03", 3, 2, 1),
    (4, "2019-08-04", 1, 4, 2),
    (5, "2019-08-04", 2, 2, 4),
    (6, "2019-08-05", 4, 1, 4)
]
orders_cols = ["order_id", "order_date", "item_id", "buyer_id", "seller_id"]
orders_df = spark.createDataFrame(orders_data, orders_cols)

# Sample data for users
users_data = [
    (1, "2019-01-01", "Lenovo"),
    (2, "2019-02-09", "Samsung"),
    (3, "2019-01-19", "LG"),
    (4, "2019-05-21", "HP")
]
users_cols = ["user_id", "join_date", "favorite_brand"]
users_df = spark.createDataFrame(users_data, users_cols)

# Sample data for items
items_data = [
    (1, "Samsung"),
    (2, "Lenovo"),
    (3, "LG"),
    (4, "HP")
]
items_cols = ["item_id", "item_brand"]
items_df = spark.createDataFrame(items_data, items_cols)

✅ Query 1: Get only sellers who sold 2 or more items (show 2nd sale details)
# Define window partitioned by seller_id and ordered by order_date
windowSpec = Window.partitionBy("seller_id").orderBy("order_date")

# Rank orders
ranked_orders = orders_df.withColumn("rnk", rank().over(windowSpec))

# Join with items and users to get brand info
query1 = (
    ranked_orders.filter(col("rnk") == 2)
    .join(items_df, "item_id")
    .join(users_df, ranked_orders.seller_id == users_df.user_id)
    .select("seller_id", "item_brand", "favorite_brand")
)

query1.show()

✅ Query 2: Show all sellers (even if < 2 sales), include YES/NO check
# Left join users with ranked_orders filtered for rnk=2
query2 = (
    users_df.alias("u")
    .join(
        ranked_orders.filter(col("rnk") == 2).alias("ro"),
        col("u.user_id") == col("ro.seller_id"),
        "left"
    )
    .join(items_df.alias("i"), col("ro.item_id") == col("i.item_id"), "left")
    .select(
        col("u.user_id").alias("seller_id"),
        col("i.item_brand"),
        col("u.favorite_brand"),
        when(col("i.item_brand") == col("u.favorite_brand"), "yes").otherwise("no").alias("item_fav_brand")
    )
)

query2.show()

✅ Query 3: Simplified output (seller_id + yes/no)
query3 = (
    users_df.alias("u")
    .join(
        ranked_orders.filter(col("rnk") == 2).alias("ro"),
        col("u.user_id") == col("ro.seller_id"),
        "left"
    )
    .join(items_df.alias("i"), col("ro.item_id") == col("i.item_id"), "left")
    .select(
        col("u.user_id").alias("seller_id"),
        when(col("i.item_brand") == col("u.favorite_brand"), "yes").otherwise("no").alias("item_fav_brand")
    )
)

query3.show()

🔹 Expected Output (based on your data)

Query 1 → Only sellers with ≥2 sales:

+---------+----------+--------------+
|seller_id|item_brand|favorite_brand|
+---------+----------+--------------+
|        1|       LG |        Lenovo|
|        4|       HP |           HP |
+---------+----------+--------------+


Query 2 → All sellers, full details + YES/NO:

+---------+----------+--------------+--------------+
|seller_id|item_brand|favorite_brand|item_fav_brand|
+---------+----------+--------------+--------------+
|        1|       LG |        Lenovo|           no |
|        2|      null|       Samsung|           no |
|        3|      null|           LG |           no |
|        4|       HP |           HP |          yes |
+---------+----------+--------------+--------------+


Query 3 → Clean final output:

+---------+--------------+
|seller_id|item_fav_brand|
+---------+--------------+
|        1|           no |
|        2|           no |
|        3|           no |
|        4|          yes |
+---------+--------------+