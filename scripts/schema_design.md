# NoSQL Schema Design — MongoDB

## Database Choice & Justification

### Why MongoDB over SQL?

| Requirement | SQL Problem | MongoDB Solution |
|---|---|---|
| **Read-heavy, low-latency** | Joins across normalized tables slow down reads | Single denormalized document = one query, no joins |
| **Per-user profile lookup** | Requires `SELECT ... JOIN ... WHERE user_id = ?` | `db.user_profiles.find({_id: userId})` — direct primary key lookup in microseconds |
| **Variable-length top-N categories** | Needs separate mapping table + pivot query | Embedded array of objects — flexible, no schema migration |
| **Nested data** | Would need JSON column or separate tables | Native support for nested documents and arrays |

### Why Not Redis?

While Redis offers fast key-value lookups, it lacks:
- Native support for nested document queries (would need manual serialization)
- A Spark connector for seamless integration (Phase 3 requires joining MongoDB data with Spark)
- Complex query capabilities (array containment, nested field access)

---

## Collections

### 1. `ecommerce.user_profiles`

**Purpose:** Store per-user behavioral affinity profiles for real-time recommendation lookups.

**Schema:**
```json
{
  "_id": "User_1",
  "top_categories": [
    { "category": "Home", "score": 144 },
    { "category": "Electronics", "score": 135 },
    { "category": "Clothing", "score": 127 },
    { "category": "Books", "score": 123 },
    { "category": "Toys", "score": 109 }
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `_id` | `string` | User identifier (unique primary key) |
| `top_categories` | `array<object>` | Top 5 categories ranked by weighted affinity score |
| `top_categories[].category` | `string` | Product category name |
| `top_categories[].score` | `int` | Aggregated weighted score (view=1, cart=3, purchase=5) |

**Indexes:**
```javascript
// Primary key — automatic (MongoDB creates on _id)
// For cross-filtering by category:
db.user_profiles.createIndex({ "top_categories.category": 1 })
```

**Query pattern:**
```javascript
// Instant lookup: get full profile for a user
db.user_profiles.find({ _id: "User_42" })
// Output: the complete document with top categories
```

**Source:** Phase 1.2 (User Affinity Aggregation) — weighted event aggregation per user.

---

### 2. `ecommerce.item_pairs`

**Purpose:** Store globally aggregated item co-occurrence frequencies for "frequently bought together" product recommendations.

**Schema:**
```json
{
  "_id": ObjectId("..."),
  "item_a": "ITEM_2436",
  "item_b": "ITEM_297",
  "co_count": 2
}
```

| Field | Type | Description |
|---|---|---|
| `_id` | `ObjectId` | Auto-generated unique identifier |
| `item_a` | `string` | First item in the pair |
| `item_b` | `string` | Second item in the pair |
| `co_count` | `int` | Number of sessions where both items were purchased together |

Pairs are unordered — `(A, B)` is stored once regardless of which item appears first alphabetically.

**Indexes:**
```javascript
db.item_pairs.createIndex({ item_a: 1 })
db.item_pairs.createIndex({ item_b: 1 })
```

**Query pattern:**
```javascript
// Find items frequently bought with a specific product
db.item_pairs.find({ item_a: "ITEM_X" }).sort({ co_count: -1 }).limit(5)
db.item_pairs.find({ item_b: "ITEM_X" }).sort({ co_count: -1 }).limit(5)
```

**Source:** Phase 1.1 (Market Basket Analysis) — item co-occurrence counting across purchase sessions.

---

## Data Flow Diagram

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌────────────────┐
│  Raw CSV     │ ──> │  Spark Phase 1   │ ──> │    MongoDB       │ ──> │  Spark Phase 3 │
│  10.8M rows  │     │  (MapReduce)     │     │  (NoSQL Store)   │     │  (Enrichment)  │
└──────────────┘     └──────────────────┘     └──────────────────┘     └────────────────┘
                           │                          │                        │
                           ├─ 1.1: item_pairs ───────>│                        │
                           │  (65,672 docs)           │                        │
                           │                          │                        │
                           ├─ 1.2: user_profiles ────>│ ── (profiles join) ───>│
                           │  (25,000 docs)           │                        │
                           │                          │                        └─> campaign.csv
                           │                          │                            (1.4M targets)
                           └──────────────────────────┘
```

## Summary

MongoDB was chosen because its **document model** perfectly matches the denormalized, read-optimized access pattern needed for real-time recommendation queries. Each user's complete behavioral profile (top categories + scores) is embedded in a single document retrievable via a primary key lookup — no joins, no complex queries, minimal latency.
