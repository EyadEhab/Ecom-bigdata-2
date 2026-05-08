import json
import os
from pymongo import MongoClient

client = MongoClient("mongodb://mongodb:27017")
db = client.ecommerce

# ─── Ingest User Affinity Profiles ───
user_affinity_dir = "/opt/spark/output/user_affinity"
user_affinity_file = None
for f in os.listdir(user_affinity_dir):
    if f.startswith("part-") and f.endswith(".json"):
        user_affinity_file = os.path.join(user_affinity_dir, f)
        break

profiles_collection = db.user_profiles
profiles_collection.delete_many({})

if user_affinity_file:
    batch = []
    with open(user_affinity_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            mongo_doc = {
                "_id": doc["user_id"],
                "top_categories": doc["top_categories"]
            }
            batch.append(mongo_doc)
            if len(batch) >= 1000:
                profiles_collection.insert_many(batch)
                batch = []
    if batch:
        profiles_collection.insert_many(batch)

total_profiles = profiles_collection.count_documents({})
print(f"Ingested {total_profiles} user profiles")

profiles_collection.create_index("top_categories.category")

# ─── Ingest Market Basket Co-occurrence Pairs ───
market_basket_dir = "/opt/spark/output/market_basket"
market_basket_file = None
for f in os.listdir(market_basket_dir):
    if f.startswith("part-") and f.endswith(".csv"):
        market_basket_file = os.path.join(market_basket_dir, f)
        break

pairs_collection = db.item_pairs
pairs_collection.delete_many({})

if market_basket_file:
    batch = []
    with open(market_basket_file, "r") as f:
        header = f.readline()
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) >= 3:
                mongo_doc = {
                    "item_a": parts[0],
                    "item_b": parts[1],
                    "co_count": int(parts[2])
                }
                batch.append(mongo_doc)
            if len(batch) >= 1000:
                pairs_collection.insert_many(batch)
                batch = []
    if batch:
        pairs_collection.insert_many(batch)

total_pairs = pairs_collection.count_documents({})
print(f"Ingested {total_pairs} item pairs")

pairs_collection.create_index("item_a")
pairs_collection.create_index("item_b")

# ─── Verify ───
print("\n=== Sample user profile ===")
sample = profiles_collection.find_one()
print(json.dumps(sample, indent=2, default=str))

print("\n=== Sample item pair ===")
sample_pair = pairs_collection.find_one()
print(json.dumps(sample_pair, indent=2, default=str))

client.close()
