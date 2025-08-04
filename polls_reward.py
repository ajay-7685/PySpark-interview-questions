# --------------------------------------------
# STEP 1: Import PySpark & create session
# --------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, round as _round

spark = SparkSession.builder \
    .appName("PollsRewardDistribution") \
    .getOrCreate()

# --------------------------------------------
# STEP 2: Create the 'polls' DataFrame
# Same as your MySQL 'polls' table
# --------------------------------------------

polls_data = [
    ('id1', 'p1', 'A', 200, '2021-12-01'),
    ('id2', 'p1', 'C', 250, '2021-12-01'),
    ('id3', 'p1', 'A', 200, '2021-12-01'),
    ('id4', 'p1', 'B', 500, '2021-12-01'),
    ('id5', 'p1', 'C', 50, '2021-12-01'),
    ('id6', 'p1', 'D', 500, '2021-12-01'),
    ('id7', 'p1', 'C', 200, '2021-12-01'),
    ('id8', 'p1', 'A', 100, '2021-12-01'),
    ('id9', 'p2', 'A', 300, '2023-01-10'),
    ('id10', 'p2', 'C', 400, '2023-01-11'),
    ('id11', 'p2', 'B', 250, '2023-01-12'),
    ('id12', 'p2', 'D', 600, '2023-01-13'),
    ('id13', 'p2', 'C', 150, '2023-01-14'),
    ('id14', 'p2', 'A', 100, '2023-01-15'),
    ('id15', 'p2', 'C', 200, '2023-01-16')
]

polls_columns = ['user_id', 'poll_id', 'poll_option_id', 'amount', 'created_date']

polls_df = spark.createDataFrame(polls_data, polls_columns)

print("✅ Polls DataFrame:")
polls_df.show()

# --------------------------------------------
# STEP 3: Create the 'poll_answers' DataFrame
# Same as your MySQL 'poll_answers' table
# --------------------------------------------

poll_answers_data = [
    ('p1', 'C'),
    ('p2', 'A')
]

poll_answers_columns = ['poll_id', 'correct_option_id']

poll_answers_df = spark.createDataFrame(poll_answers_data, poll_answers_columns)

print("✅ Poll Answers DataFrame:")
poll_answers_df.show()

# --------------------------------------------
# STEP 4: Join for verifying (optional)
# Check what each user's vote looks like with the correct option
# --------------------------------------------

joined_df = polls_df.join(
    poll_answers_df,
    on="poll_id",
    how="inner"
)

print("✅ Joined DataFrame (each vote + correct answer):")
joined_df.show()

# --------------------------------------------
# STEP 5: Get wrong answer total (SQL version)
# --------------------------------------------

wrong_sum_sql = """
SELECT 
  p.poll_id,
  SUM(p.amount) AS wrong_answer_total
FROM polls p
JOIN poll_answers a
  ON p.poll_id = a.poll_id
WHERE p.poll_option_id != a.correct_option_id
GROUP BY p.poll_id
"""

print("✅ Wrong answer total (Spark SQL version):")
wrong_sum_df_sql = spark.sql(wrong_sum_sql)
wrong_sum_df_sql.show()

# --------------------------------------------
# STEP 6: Get wrong answer total (PySpark DataFrame version)
# Same logic as above but with PySpark API — no SQL string!
# --------------------------------------------

wrong_sum_df = joined_df.filter(
    col("poll_option_id") != col("correct_option_id")
).groupBy(
    "poll_id"
).agg(
    _sum("amount").alias("wrong_answer_total")
)

print("✅ Wrong answer total (PySpark DataFrame version):")
wrong_sum_df.show()

# --------------------------------------------
# STEP 7: Compute proportional share for correct voters
# Same logic as MySQL version
# --------------------------------------------

# Calculate total correct sum & wrong sum per poll
summary_df = joined_df.groupBy("poll_id").agg(
    _sum(when(col("poll_option_id") == col("correct_option_id"), col("amount"))).alias("correct_sum"),
    _sum(when(col("poll_option_id") != col("correct_option_id"), col("amount"))).alias("wrong_sum")
)

# Join back to get each user's correct amount & share
result_df = joined_df.join(
    summary_df,
    on="poll_id",
    how="inner"
).filter(
    col("poll_option_id") == col("correct_option_id")
).withColumn(
    "user_share",
    _round(col("amount") / col("correct_sum") * col("wrong_sum"), 2)
).select(
    "user_id",
    "poll_id",
    col("amount").alias("user_correct_amount"),
    "correct_sum",
    "wrong_sum",
    "user_share"
)

print("✅ Final user reward share for correct voters:")
result_df.show()
