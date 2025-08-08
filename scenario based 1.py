from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_set, collect_list, concat_ws, rank
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Step 1: Distinct resources per user
distinct_resources = visits_df.select("name", "resources").distinct()

# Step 2: Aggregate distinct resources per user
agg_resources = distinct_resources.groupBy("name").agg(
    concat_ws(",", collect_set("resources")).alias("used_resources")
)

# Step 3: Total visits and list of all resources per user
total_visits = visits_df.groupBy("name").agg(
    count("*").alias("total_visits"),
    concat_ws(",", collect_list("resources")).alias("resources_used")
)

# Step 4: Count visits per floor and rank floors per user
floor_counts = visits_df.groupBy("name", "floor").agg(
    count("*").alias("no_of_floor_visit")
)

# Apply ranking (most visited floor = rank 1)
window_spec = Window.partitionBy("name").orderBy(col("no_of_floor_visit").desc())
floor_ranked = floor_counts.withColumn("rn", rank().over(window_spec))

# Step 5: Filter most visited floor (rn = 1)
most_visited_floor = floor_ranked.filter(col("rn") == 1)

# Step 6: Join all results
final_result = (
    most_visited_floor.alias("fv")
    .join(total_visits.alias("tv"), col("fv.name") == col("tv.name"))
    .join(agg_resources.alias("ar"), col("fv.name") == col("ar.name"))
    .select(
        col("fv.name"),
        col("fv.floor").alias("most_visited_floor"),
        col("tv.total_visits"),
        col("ar.used_resources")
    )
)

# Show the result
final_result.show()
