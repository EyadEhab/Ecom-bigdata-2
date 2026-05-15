from pyspark.sql import SparkSession, functions as F, Window


spark = SparkSession.builder \
    .appName("UserAffinityAggregation") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("escape", "\"") \
    .csv("/opt/spark/data/ecommerce_logs.csv")

df = df.filter(F.col("event_type").isin("view", "cart", "purchase")) \
    .withColumn("product_metadata", F.expr("from_json(product_metadata, 'category STRING, brand STRING, stock INT')"))

weight_map = F.when(F.col("event_type") == "view", 1) \
    .when(F.col("event_type") == "cart", 3) \
    .when(F.col("event_type") == "purchase", 5)

user_category_scores = df.filter(F.col("product_metadata.category").isNotNull()) \
    .withColumn("weight", weight_map) \
    .groupBy("user_id", F.col("product_metadata.category").alias("category")) \
    .agg(F.sum("weight").alias("score"))

user_affinity = user_category_scores \
    .withColumn(
        "rank",
        F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("score").desc()))
    ) \
    .filter(F.col("rank") <= 3) \
    .groupBy("user_id") \
    .agg(
        F.collect_list(
            F.struct(F.col("category"), F.col("score"))
        ).alias("top_categories")
    )

user_affinity.show(10, truncate=False, vertical=True)

user_affinity.coalesce(1) \
    .write \
    .mode("overwrite") \
    .json("/opt/spark/output/user_affinity")

total_users = user_affinity.count()
print(f"Total users with affinity profiles: {total_users}")

spark.stop()
