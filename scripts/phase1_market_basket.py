from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType
import itertools

spark = SparkSession.builder \
    .appName("MarketBasketAnalysis") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("escape", "\"") \
    .csv("/opt/spark/data/ecommerce_logs.csv")

purchases = df.filter(F.col("event_type") == "purchase") \
    .select("session_id", "product_id") \
    .dropDuplicates()

def generate_pairs(items):
    items = sorted(set(items))
    pairs = []
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            pairs.append((items[i], items[j]))
    return pairs

generate_pairs_udf = F.udf(generate_pairs, ArrayType(
    StructType([
        StructField("item_a", StringType(), False),
        StructField("item_b", StringType(), False)
    ])
))

session_pairs = purchases.groupBy("session_id") \
    .agg(F.collect_list("product_id").alias("items")) \
    .filter(F.size("items") >= 2) \
    .withColumn("pairs", F.explode(generate_pairs_udf("items"))) \
    .select("pairs.item_a", "pairs.item_b")

pair_counts = session_pairs.groupBy("item_a", "item_b") \
    .agg(F.count("*").alias("co_count")) \
    .orderBy(F.col("co_count").desc())

pair_counts.show(20, truncate=False)

pair_counts.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark/output/market_basket")

total_pairs = pair_counts.count()
print(f"Total unique co-occurring pairs found: {total_pairs}")

spark.stop()
