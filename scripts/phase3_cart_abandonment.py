from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("CartAbandonmentRecovery") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("escape", "\"") \
    .csv("/opt/spark/data/ecommerce_logs.csv")

session_has_event = df.groupBy("session_id") \
    .agg(F.collect_set("event_type").alias("event_types")) \
    .cache()

abandoned_sessions = session_has_event.filter(
    F.array_contains(F.col("event_types"), "cart") &
    (F.array_contains(F.col("event_types"), "purchase") == False)
)

abandoned_sessions.write \
    .mode("overwrite") \
    .parquet("/tmp/abandoned_sessions")

spark.read.parquet("/tmp/abandoned_sessions").createOrReplaceTempView("abandoned")

df.createOrReplaceTempView("logs")

cart_events = spark.sql("""
    SELECT DISTINCT l.session_id, l.user_id, l.product_id,
           get_json_object(l.product_metadata, '$.category') AS category
    FROM logs l
    INNER JOIN abandoned a ON l.session_id = a.session_id
    WHERE l.event_type = 'cart'
      AND l.user_id IS NOT NULL
      AND get_json_object(l.product_metadata, '$.category') IS NOT NULL
""")

cart_events.createOrReplaceTempView("cart_events")

client = MongoClient("mongodb://mongodb:27017")
profiles = list(client.ecommerce.user_profiles.find())
client.close()
print(f"Loaded {len(profiles)} user profiles")

profile_data = [(p["_id"], [tc["category"] for tc in p.get("top_categories", [])]) for p in profiles]
profile_schema = StructType([
    StructField("user_id", StringType()),
    StructField("top_categories", ArrayType(StringType()))
])
profiles_df = spark.createDataFrame(profile_data, schema=profile_schema)
profiles_df.createOrReplaceTempView("profiles")

campaign = spark.sql("""
    SELECT c.user_id, c.session_id, c.product_id, c.category,
           CASE WHEN array_contains(p.top_categories, c.category)
                THEN 'High_Discount'
                ELSE 'Standard_Reminder'
           END AS campaign_type
    FROM cart_events c
    INNER JOIN profiles p ON c.user_id = p.user_id
""")

campaign.coalesce(2).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark/output/campaign")

result = spark.read.csv("/opt/spark/output/campaign", header=True)
result.groupBy("campaign_type").count().show()
print(f"Total targets: {result.count()}")
result.show(5, truncate=False)

spark.stop()
