from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, unix_timestamp, round as _round, expr

# 1️⃣ Start Spark session
spark = SparkSession.builder \
    .appName("FlightScheduleDB") \
    .getOrCreate()

# ----------------------------------------
# 2️⃣ Create 'ports' DataFrame
# ----------------------------------------

ports_data = [
    ('EWR', 'New York'),
    ('HND', 'Tokyo'),
    ('JFK', 'New York'),
    ('KIX', 'Osaka'),
    ('LAX', 'Los Angeles'),
    ('LGA', 'New York'),
    ('NRT', 'Tokyo'),
    ('ORD', 'Chicago'),
    ('SFO', 'San Francisco')
]

ports_columns = ['port_code', 'city_name']

ports_df = spark.createDataFrame(ports_data, ports_columns)

print("✅ Ports:")
ports_df.show()

# ----------------------------------------
# 3️⃣ Create 'flights' DataFrame
# ----------------------------------------

flights_data = [
    (1, 'JFK', 'HND', '2025-06-15 06:00:00', '2025-06-15 18:00:00'),
    (2, 'JFK', 'LAX', '2025-06-15 07:00:00', '2025-06-15 10:00:00'),
    (3, 'LAX', 'NRT', '2025-06-15 10:00:00', '2025-06-15 22:00:00'),
    (4, 'JFK', 'LAX', '2025-06-15 08:00:00', '2025-06-15 11:00:00'),
    (5, 'LAX', 'KIX', '2025-06-15 11:30:00', '2025-06-15 22:00:00'),
    (6, 'LGA', 'ORD', '2025-06-15 09:00:00', '2025-06-15 11:00:00'),
    (7, 'ORD', 'HND', '2025-06-15 11:30:00', '2025-06-15 23:30:00'),
    (8, 'EWR', 'SFO', '2025-06-15 09:00:00', '2025-06-15 13:00:00'),
    (9, 'LAX', 'HND', '2025-06-15 13:00:00', '2025-06-15 23:00:00'),
    (10, 'KIX', 'NRT', '2025-06-15 08:00:00', '2025-06-15 10:00:00')
]

flights_columns = ['flight_id', 'start_port', 'end_port', 'start_time', 'end_time']

flights_df = spark.createDataFrame(flights_data, flights_columns)

print("✅ Flights:")
flights_df.show(truncate=False)

# ----------------------------------------
# 4️⃣ Build 'flight_details'
# JOIN flights with ports to get city names
# ----------------------------------------

flight_details = flights_df \
    .join(ports_df.withColumnRenamed("port_code", "start_port_code").withColumnRenamed("city_name", "start_city"),
          flights_df.start_port == col("start_port_code"), "inner") \
    .join(ports_df.withColumnRenamed("port_code", "end_port_code").withColumnRenamed("city_name", "end_city"),
          flights_df.end_port == col("end_port_code"), "inner") \
    .select(
        "flight_id",
        "start_port",
        "start_city",
        "end_port",
        "end_city",
        "start_time",
        "end_time"
    )

print("✅ Flight Details:")
flight_details.show(truncate=False)

# ----------------------------------------
# 5️⃣ Direct flights: New York -> Tokyo
# ----------------------------------------

direct_flights = flight_details \
    .filter(
        (col("start_city") == "New York") & (col("end_city") == "Tokyo")
    ) \
    .withColumn("middle_city", expr("NULL")) \
    .withColumn(
        "time_taken",
        (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60
    ) \
    .select(
        col("start_city").alias("trip_start_city"),
        col("middle_city"),
        col("end_city").alias("trip_end_city"),
        col("flight_id").cast("string").alias("flight_id"),
        col("time_taken").cast("int")
    )

print("✅ Direct Flights (New York -> Tokyo):")
direct_flights.show(truncate=False)

# ----------------------------------------
# 6️⃣ Connecting flights: New York -> X -> Tokyo
# ----------------------------------------

# Join flight_details with itself for connections
a = flight_details.alias("a")
b = flight_details.alias("b")

connections = a.join(
    b,
    (a.end_city == b.start_city) &
    (a.start_city == "New York") &
    (b.end_city == "Tokyo") &
    (unix_timestamp(b.start_time) >= unix_timestamp(a.end_time))
)

connecting_flights = connections.select(
    col("a.start_city").alias("trip_start_city"),
    col("a.end_city").alias("middle_city"),
    col("b.end_city").alias("trip_end_city"),
    concat_ws(";", col("a.flight_id").cast("string"), col("b.flight_id").cast("string")).alias("flight_id"),
    ((unix_timestamp(col("b.end_time")) - unix_timestamp(col("a.start_time"))) / 60).cast("int").alias("time_taken")
)

print("✅ Connecting Flights (New York -> X -> Tokyo):")
connecting_flights.show(truncate=False)

# ----------------------------------------
# 7️⃣ Final Result: UNION of direct + connecting
# ----------------------------------------

final_routes = connecting_flights.unionByName(direct_flights)

print("✅ Final Trip Options (Direct + Connecting):")
final_routes.show(truncate=False)
