from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FriendScoreCalculation") \
    .getOrCreate()

# Sample data for person table
person_data = [
    (1, "Alice", 88),
    (2, "Bob", 11),
    (3, "Davis", 27),
    (4, "Tara", 45),
    (5, "John", 63)
]
person_columns = ["personid", "name", "score"]

person_df = spark.createDataFrame(person_data, person_columns)

# Sample data for friend table
friend_data = [
    (1, 2),
    (2, 1),
    (2, 3),
    (3, 1),
    (3, 4),
    (4, 5),
    (4, 3),
    (5, 4)
]
friend_columns = ["personid", "friendid"]

friend_df = spark.createDataFrame(friend_data, friend_columns)

# -------------------------------
# Step 1: Join friend table with person table to get friend's score
# -------------------------------
friend_scores_df = friend_df.join(
    person_df,                      # Join with person_df to get friend's score
    friend_df.friendid == person_df.personid,
    "inner"
)

# -------------------------------
# Step 2: Group by the main person to get total_friend_score and no_of_friends
# -------------------------------
score_details_df = friend_scores_df.groupBy(friend_df.personid) \
    .agg(
        _sum("score").alias("total_friend_score"),  # Sum of friends' scores
        _count("*").alias("no_of_friends")          # Number of friends
    ) \
    .filter(col("total_friend_score") > 100)        # Filter where total score > 100

# -------------------------------
# Step 3: Join back to person_df to get the person's name
# -------------------------------
final_df = score_details_df.join(
    person_df,
    score_details_df.personid == person_df.personid,
    "inner"
).select(
    score_details_df.personid,
    col("name").alias("person_name"),
    col("no_of_friends"),
    col("total_friend_score")
)

# Show the result
final_df.show()
