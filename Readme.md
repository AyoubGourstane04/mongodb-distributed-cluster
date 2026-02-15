
# Project Playbook: Distributed Marketplace Catalog

## Project Context

### Goal

Deploy a **highly available, sharded MongoDB infrastructure** for a marketplace with millions of products.

### Key Requirements Addressed

* **Availability:** Replica Sets for shards and config servers
* **Scalability:** 3 distinct shards
* **Performance:**

  * `category_id` as the shard key
  * Composite Indexes for sorting/filtering

---

# Phase 1: Infrastructure Initialization

Before creating the database, we must launch the physical nodes and link them together.

---

## **Step 1: Start the Containerized Cluster**

Run:

```bash
docker-compose up -d
```

### What this does

Starts 6+ containers:

* `configsvr`: Stores cluster metadata
* `shard1`, `shard2`, `shard3`: Storage nodes
* `mongos`: The router that applications connect to

---

## **Step 2: Initialize the Config Server**

```bash
docker exec -it $(docker ps -qf "name=configsvr") mongosh
```

Inside `mongosh`:

```js
rs.initiate({
  _id: "configReplSet",
  configsvr: true,
  members: [{ _id: 0, host: "configsvr:27017" }]
})
```

### Explanation

* `configsvr: true` tells MongoDB that this is **not** a normal database.
* The `mongos` router cannot function without metadata from the config server.

---

## **Step 3: Initialize Data Shards**

Convert each storage container into a Replica Set.

### **Shard 1**

```bash
docker exec -it mongodb-sharded-shard1-1 mongosh
```

```js
rs.initiate({
  _id: "shard1ReplSet",
  members: [{ _id: 0, host: "shard1:27017" }]
})
```

### **Shard 2**

```bash
docker exec -it mongodb-sharded-shard2-1 mongosh
```

```js
rs.initiate({
  _id: "shard2ReplSet",
  members: [{ _id: 0, host: "shard2:27017" }]
})
```

### **Shard 3**

```bash
docker exec -it mongodb-sharded-shard3-1 mongosh
```

```js
rs.initiate({
  _id: "shard3ReplSet",
  members: [{ _id: 0, host: "shard3:27017" }]
})
```

### Why Replica Sets?

Sharding **requires** the Oplog → only available in Replica Set mode.

---

## **Step 4: Connect Shards to the Router (mongos)**

```bash
docker exec -it mongodb-sharded-mongos-1 mongosh
```

Inside `mongos`:

```js
sh.addShard("shard1ReplSet/shard1:27017")
sh.addShard("shard2ReplSet/shard2:27017")
sh.addShard("shard3ReplSet/shard3:27017")
```

### What this does

Updates the Config Server’s metadata → the cluster now officially has 3 shards.

---

# Phase 2: Schema Design & Sharding Strategy

⚠️ All commands from this point forward run inside the **mongos shell**.

---

## **Step 5: Stop the Balancer (Optimization Prep)**

```js
sh.stopBalancer()
```

### Why?

Prevents automatic data migration while performing manual chunk operations.

---

## **Step 6: Database & Index Setup (Requirement A & C)**

```js
use marketplace

// Enable sharding at the database level
sh.enableSharding("marketplace")

// Composite indexes
db.products.createIndex({ "category_id": 1, "price": 1 })
db.products.createIndex({ "category_id": 1, "rating": -1 })
db.products.createIndex({ "attributes.k": 1, "attributes.v": 1 })
db.products.createIndex({ "name": "text" })
```

### Why Composite Indexes?

Improves multi-criteria searches and removes in-memory sorting.

---

## **Step 7: Apply Sharding Key (Requirement B)**

```js
sh.shardCollection("marketplace.products", { category_id: 1, _id: 1 })
```

### Why this sharding strategy?

* `category_id` = Targeted queries (e.g., "Electronics")
* `_id` = Ensures uniqueness within each shard

---

# Phase 3: Pre-Splitting (Advanced Optimization)

If you import data now, it will all land in Shard 1.
We manually pre-split chunks to distribute load.

---

## **Step 8: Create Logical Splits**

```js
use admin
for (var i = 1; i < 100; i++) {
   db.adminCommand({ 
      split: "marketplace.products",
      middle: { category_id: i, _id: MinKey }
   });
}
```

### What this does

Defines empty chunk boundaries for categories 1 → 99.

---

## **Step 9: Distribute Chunks (Round Robin)**

```js
var shards = ["shard1ReplSet", "shard2ReplSet", "shard3ReplSet"];
for (var i = 0; i < 100; i++) {
    var targetShard = shards[i % shards.length];
    db.adminCommand({
       moveChunk: "marketplace.products", 
       find: { category_id: i, _id: MinKey },
       to: targetShard
    });
}
```

### Expected Distribution

* Shard 1 → categories 1, 4, 7…
* Shard 2 → categories 2, 5, 8…
* Shard 3 → categories 3, 6, 9…

➡️ During import, **all 3 shards write simultaneously → x3 throughput**.

---

# Phase 4: Data Injection

### Import data

```bash
docker cp generated_data\products_part_1.json mongodb-sharded-mongos-1:/products.json 

docker exec -it mongodb-sharded-mongos-1 mongoimport --host localhost --port 27017 --db marketplace --collection products --file /products.json --jsonArray

docker cp generated_data\products_part_2.json mongodb-sharded-mongos-1:/products.json 

docker exec -it mongodb-sharded-mongos-1 mongoimport --host localhost --port 27017 --db marketplace --collection products --file /products.json --jsonArray

docker cp generated_data\products_part_3.json mongodb-sharded-mongos-1:/products.json 

docker exec -it mongodb-sharded-mongos-1 mongoimport --host localhost --port 27017 --db marketplace --collection products --file /products.json --jsonArray

docker cp generated_data\products_part_4.json mongodb-sharded-mongos-1:/products.json 

docker exec -it mongodb-sharded-mongos-1 mongoimport --host localhost --port 27017 --db marketplace --collection products --file /products.json --jsonArray

docker cp generated_data\categories.json mongodb-sharded-mongos-1:/categories.json

docker exec -it mongodb-sharded-mongos-1 mongoimport --host localhost --port 27017 --db marketplace --collection categories --file /categories.json --jsonArray

docker cp generated_data\vendors.json mongodb-sharded-mongos-1:/vendors.json

docker exec -it mongodb-sharded-mongos-1 mongoimport --host localhost --port 27017 --db marketplace --collection vendors --file /vendors.json --jsonArray
```

---

# Phase 5: Verification & Maintenance

## **Step 10: Restart the Balancer**

```js
sh.startBalancer()
```

---

## **Step 11: Verify Distribution (Requirement D)**

```js
db.products.getShardDistribution()
```

Expected output → Roughly:

* **33%** on Shard 1
* **33%** on Shard 2
* **33%** on Shard 3

---

# Phase 6: Monitoring & Observability Stack

Your `docker-compose.yml` includes a complete observability pipeline:

---

## **1. Percona MongoDB Exporter (mongo-exporter)**

**Role:** Translator → exposes MongoDB stats in Prometheus format.
**Mode:** `--discovering-mode` (auto-detects shards).

---

## **2. Prometheus**

**Role:** Time-series datastore for metrics.
**Access:**
👉 [http://localhost:9090](http://localhost:9090)

Prometheus scrapes exporter metrics every few seconds.

---

## **3. Grafana**

**Role:** Visual dashboard interface.
**Access:**
👉 [http://localhost:3000](http://localhost:3000)
**User:** `admin`
**Password:** `password123`

### Setup inside Grafana:

1. **Add Data Source** → Prometheus → `http://prometheus:9090`
2. **Import Dashboard** → IDs like `16490` or `2583`

---

## **4. Test Runner (test-runner)**

**Role:** Stress-testing engine
Simulates thousands of shoppers to test your cluster under load.
Exposes: **port 8001**

---
