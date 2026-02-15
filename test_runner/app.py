import time
import os
import sys
import random
import traceback
from pymongo import MongoClient
from prometheus_client import start_http_server, Gauge, Counter

# --- CONFIGURATION ---
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongos:27017')
DB_NAME = "marketplace"
COLLECTION = "products"

# --- PROMETHEUS METRICS ---
TEST_DURATION = Gauge('mongo_test_duration_seconds', 'Time taken to run the test', ['test_name', 'test_type'])
DOCS_EXAMINED = Gauge('mongo_test_docs_examined', 'Total documents scanned', ['test_name'])
DOCS_RETURNED = Gauge('mongo_test_docs_returned', 'Total documents returned', ['test_name'])
DOCS_MODIFIED = Gauge('mongo_test_docs_modified', 'Total documents modified', ['test_name'])
TEST_ERRORS = Counter('mongo_test_errors_total', 'Total test failures', ['test_name'])
WINNING_PLAN = Gauge('mongo_test_winning_plan', '1=IXSCAN (Good), 0=COLLSCAN (Bad)', ['test_name'])

# --- NEW METRIC: TOPOLOGY ---
# Records how many shards were actually involved in the operation (1 = Targeted, >1 = Broadcast)
SHARDS_HIT = Gauge('mongo_test_shards_hit', 'Number of shards contacted', ['test_name'])

# --- SHARD MAPPING ---
SHARD_1_IDS = [i for i in range(100) if i % 3 == 0]
SHARD_2_IDS = [i for i in range(100) if i % 3 == 1]
SHARD_3_IDS = [i for i in range(100) if i % 3 == 2]

def log(message):
    print(message)
    sys.stdout.flush()

def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

def count_shards_from_explain(explain_json):
    """Parses explain output to see how many shards were involved."""
    try:
        # Check for executionStages
        stages = explain_json.get('executionStats', {}).get('executionStages', {})
        
        # If there is a SHARD_MERGE stage, it usually lists the shards
        if 'shards' in stages:
            return len(stages['shards'])
        
        # Check if top-level 'shards' key exists (common in aggregations)
        if 'shards' in explain_json:
            return len(explain_json['shards'])

        # If no shards list, it was likely optimized to a single shard
        return 1
    except:
        return 0

def get_agg_stats(explain_json):
    """Helper to sum up examined/returned docs from Sharded Aggregation Explain."""
    total_examined = 0
    total_returned = 0

    try:
        # CASE A: Sharded Response (Has 'shards' dict)
        if 'shards' in explain_json:
            for shard_name, shard_data in explain_json['shards'].items():
                try:
                    # Stages -> Cursor -> executionStats
                    if 'stages' in shard_data:
                        cursor_stage = shard_data['stages'][0].get('$cursor', {})
                        stats = cursor_stage.get('executionStats', {})
                        total_examined += stats.get('totalDocsExamined', 0)
                        total_returned += stats.get('nReturned', 0)
                except:
                    pass
        
        # CASE B: Single Shard / Unsharded Response
        elif 'stages' in explain_json:
             cursor_stage = explain_json['stages'][0].get('$cursor', {})
             stats = cursor_stage.get('executionStats', {})
             total_examined = stats.get('totalDocsExamined', 0)
             total_returned = stats.get('nReturned', 0)
             
    except Exception:
        pass
        
    return total_examined, total_returned

def run_tests():
    log("🚀 Test Runner Started... Waiting for MongoDB connection...")
    time.sleep(10) 
    
    try:
        db = get_db()
        col = db[COLLECTION]
        count = col.count_documents({})
        log(f"🚀 Connected. Found {count} documents.")
    except Exception:
        log(traceback.format_exc())
        return

    log("Starting 6-Stage Stress Test Loop...")

    while True:
        # ==============================================================================
        # TEST 1: WRITE STRESS (Shard 1 Targeted)
        # ==============================================================================
        try:
            start_time = time.time()
            target_cat = random.choice(SHARD_1_IDS)
            
            res = col.update_one(
                {"category_id": target_cat, "price": {"$lt": 5000}}, 
                {"$inc": {"price": 0.01}}
            )
            duration = time.time() - start_time
            
            TEST_DURATION.labels(test_name='shard1_write', test_type='write').set(duration)
            DOCS_MODIFIED.labels(test_name='shard1_write').set(res.modified_count)
            # Writes don't return "Examined" stats easily, but we set match count as returned for visibility
            DOCS_RETURNED.labels(test_name='shard1_write').set(res.matched_count)
            DOCS_EXAMINED.labels(test_name='shard1_write').set(0) # Not available without heavy explain
            
            WINNING_PLAN.labels(test_name='shard1_write').set(1)
            SHARDS_HIT.labels(test_name='shard1_write').set(1)

        except Exception:
            TEST_ERRORS.labels(test_name='shard1_write').inc()

        # ==============================================================================
        # TEST 2: READ/SORT (Shard 2 Targeted)
        # ==============================================================================
        try:
            start_time = time.time()
            target_cat = random.choice(SHARD_2_IDS)
            
            explain = col.find({"category_id": target_cat, "rating": {"$gt": 3}})\
                         .sort("price", -1)\
                         .limit(50)\
                         .explain()
            
            duration = time.time() - start_time
            stats = explain['executionStats']
            shards_count = count_shards_from_explain(explain)
            
            plan_value = 1 if "IXSCAN" in str(explain) else 0
            
            TEST_DURATION.labels(test_name='shard2_read', test_type='read').set(duration)
            DOCS_EXAMINED.labels(test_name='shard2_read').set(stats['totalDocsExamined'])
            DOCS_RETURNED.labels(test_name='shard2_read').set(stats['nReturned'])
            WINNING_PLAN.labels(test_name='shard2_read').set(plan_value)
            SHARDS_HIT.labels(test_name='shard2_read').set(shards_count)

        except Exception:
            TEST_ERRORS.labels(test_name='shard2_read').inc()

        # ==============================================================================
        # TEST 3: AGGREGATION (Shard 3 Targeted)
        # ==============================================================================
        try:
            start_time = time.time()
            target_cat = random.choice(SHARD_3_IDS)
            
            pipeline = [
                {"$match": {"category_id": target_cat}},
                {"$group": {"_id": "$vendor_id", "avgPrice": {"$avg": "$price"}}}
            ]
            
            explain = db.command('explain', {'aggregate': COLLECTION, 'pipeline': pipeline, 'cursor': {}}, verbosity='executionStats')
            duration = time.time() - start_time
            
            plan_value = 1 if "IXSCAN" in str(explain) else 0
            shards_count = count_shards_from_explain(explain)
            
            # Use new helper to safely get stats
            examined, returned = get_agg_stats(explain)

            TEST_DURATION.labels(test_name='shard3_agg', test_type='aggregation').set(duration)
            DOCS_EXAMINED.labels(test_name='shard3_agg').set(examined)
            DOCS_RETURNED.labels(test_name='shard3_agg').set(returned)
            WINNING_PLAN.labels(test_name='shard3_agg').set(plan_value)
            SHARDS_HIT.labels(test_name='shard3_agg').set(shards_count)

        except Exception:
            TEST_ERRORS.labels(test_name='shard3_agg').inc()

        # ==============================================================================
        # TEST 4: SCATTER-GATHER READ
        # ==============================================================================
        try:
            start_time = time.time()
            explain = col.find({"rating": {"$gt": 3}})\
                         .sort("price", -1)\
                         .limit(50)\
                         .explain()
            
            duration = time.time() - start_time
            stats = explain['executionStats']
            shards_count = count_shards_from_explain(explain)

            plan_value = 1 if "IXSCAN" in str(explain) else 0

            TEST_DURATION.labels(test_name='scatter_read', test_type='read').set(duration)
            DOCS_EXAMINED.labels(test_name='scatter_read').set(stats['totalDocsExamined'])
            DOCS_RETURNED.labels(test_name='scatter_read').set(stats['nReturned'])
            WINNING_PLAN.labels(test_name='scatter_read').set(plan_value)
            SHARDS_HIT.labels(test_name='scatter_read').set(shards_count)

        except Exception:
            TEST_ERRORS.labels(test_name='scatter_read').inc()

        # ==============================================================================
        # TEST 5: SCATTER-GATHER AGGREGATION
        # ==============================================================================
        try:
            start_time = time.time()
            target_vendor = random.randint(0, 99)
            
            pipeline = [
                {"$match": {"vendor_id": target_vendor}},
                {"$group": {"_id": "$category_id", "totalProducts": {"$sum": 1}}}
            ]
            
            explain = db.command('explain', {'aggregate': COLLECTION, 'pipeline': pipeline, 'cursor': {}}, verbosity='executionStats')
            duration = time.time() - start_time
            
            plan_value = 1 if "IXSCAN" in str(explain) else 0
            shards_count = count_shards_from_explain(explain)
            
            # Use new helper to safely get stats (sums up all shards)
            examined, returned = get_agg_stats(explain)

            TEST_DURATION.labels(test_name='scatter_agg', test_type='aggregation').set(duration)
            DOCS_EXAMINED.labels(test_name='scatter_agg').set(examined)
            DOCS_RETURNED.labels(test_name='scatter_agg').set(returned)
            WINNING_PLAN.labels(test_name='scatter_agg').set(plan_value)
            SHARDS_HIT.labels(test_name='scatter_agg').set(shards_count)

        except Exception:
            TEST_ERRORS.labels(test_name='scatter_agg').inc()

        # ==============================================================================
        # TEST 6: SCATTER-GATHER WRITE
        # ==============================================================================
        try:
            start_time = time.time()
            target_prod_id = random.randint(0, 10000) 
            
            res = col.update_many(
                            {"product_id": target_prod_id}, 
                            {"$inc": {"price": 0.01}}
            )
            duration = time.time() - start_time
            
            TEST_DURATION.labels(test_name='scatter_write', test_type='write').set(duration)
            DOCS_MODIFIED.labels(test_name='scatter_write').set(res.modified_count)
            
            # --- FIX: Set these to avoid "No Data" ---
            # Matched count acts as "Returned" (docs found)
            DOCS_RETURNED.labels(test_name='scatter_write').set(res.matched_count)
            # Write results don't give "Examined" easily, so set to 0 to prevent gaps
            DOCS_EXAMINED.labels(test_name='scatter_write').set(0)
            
            WINNING_PLAN.labels(test_name='scatter_write').set(0)
            SHARDS_HIT.labels(test_name='scatter_write').set(3)

        except Exception:
            TEST_ERRORS.labels(test_name='scatter_write').inc()

        log(f"✅ Loop Complete. Shards Hit Metrics Updated.")
        time.sleep(5)

if __name__ == '__main__':
    start_http_server(8001)
    log("📡 Metrics Server running on port 8001")
    run_tests()