import argparse
from pymongo import MongoClient

def query_user_profile(user_id):
    client = MongoClient("mongodb://mongodb:27017")
    db = client.ecommerce

    profile = db.user_profiles.find_one({"_id": user_id})
    if not profile:
        print(f"User '{user_id}' not found.")
        client.close()
        return

    print(f"\n{'='*50}")
    print(f"  User Profile: {profile['_id']}")
    print(f"{'='*50}")
    print(f"\n  Top Affinity Categories (weighted scores):")
    print(f"  {'-' * 40}")
    for i, cat in enumerate(profile.get("top_categories", []), 1):
        print(f"  {i}. {cat['category']:15s} → score: {cat['score']}")
    print(f"  {'-' * 40}")

    client.close()


def query_item_pairs(product_id):
    client = MongoClient("mongodb://mongodb:27017")
    db = client.ecommerce

    pairs = list(db.item_pairs.find(
        {"$or": [{"item_a": product_id}, {"item_b": product_id}]}
    ).sort("co_count", -1).limit(5))

    if not pairs:
        print(f"\n  No co-occurrence data for '{product_id}'.")
        client.close()
        return

    print(f"\n  Frequently bought with '{product_id}':")
    print(f"  {'-' * 40}")
    for p in pairs:
        other = p["item_b"] if p["item_a"] == product_id else p["item_a"]
        print(f"  {other:15s} → co-occurrence count: {p['co_count']}")
    print(f"  {'-' * 40}")

    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MongoDB Query Demo — User Profile & Item Recommendations")
    parser.add_argument("--user", default="User_1", help="User ID to look up (default: User_1)")
    parser.add_argument("--item", help="Product ID to find frequently-bought-together items")
    args = parser.parse_args()

    query_user_profile(args.user)

    if args.item:
        query_item_pairs(args.item)
