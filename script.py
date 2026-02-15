import json
import os
import random
import time
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict

# --- Configuration (Randomized Generation) ---
NUM_CATEGORIES = 100
VENDOR_COUNT = 100
ATTRIBUTES_PER_PRODUCT = 3
MAX_WORKERS = os.cpu_count() or 4
SPLIT_PARTS = 4
PRODUCTS_PER_FILE = 2500000  # 2.5 million documents per output file
TOTAL_PRODUCTS = PRODUCTS_PER_FILE * SPLIT_PARTS  # 10 million total

# --- File Paths ---
OUTPUT_DIR = "generated_data"
VENDORS_FILE = os.path.join(OUTPUT_DIR, "vendors.json")
CATEGORIES_FILE = os.path.join(OUTPUT_DIR, "categories.json")

# --- Utility Functions: Python "Faker" (unchanged) ---

WORDS = ["Electronics", "Apparel", "HomeGoods", "Tools", "Books", "Software", "Sporting"]
PHRASES = ["Ultimate Pro Gadget", "Smart Home Device", "Vintage Look Accessory", "High Performance Tool", "Economical Choice"]
RAMS = [8, 16, 32, 64]
STORAGES = [128, 256, 512, 1024]
COLORS = ["Red", "Blue", "Green", "Black", "White", "Silver"]

def get_random_word():
    return random.choice(WORDS)

def get_random_phrase():
    return random.choice(PHRASES)

def generate_attribute(attribute_index):
    if attribute_index == 0:
        return {"k": "RAM", "v": f"{random.choice(RAMS)}GB"}
    elif attribute_index == 1:
        return {"k": "Storage", "v": f"{random.choice(STORAGES)}GB SSD"}
    else:
        return {"k": "Color", "v": random.choice(COLORS)}

# --- Core Generation Logic (REVISED) ---

def generate_product_json(global_product_id):
    """Generates a single product dictionary with a randomly selected category ID."""
    
    attributes = [generate_attribute(i) for i in range(ATTRIBUTES_PER_PRODUCT)]
    vendor_id = random.randrange(VENDOR_COUNT)
    
    # CRITICAL CHANGE: Randomly choose Category ID for *every* product
    category_id = random.randrange(NUM_CATEGORIES)
    
    product = {
        "name": get_random_phrase(),
        "price": round(random.uniform(10.0, 5000.0), 2),
        "rating": round(random.uniform(3.0, 5.0), 1),
        "attributs": attributes,
        "category_id": category_id, # This ID is now random/interleaved
        "vendor_id": vendor_id,
        "product_id": global_product_id
    }
    return product, vendor_id, category_id


def generate_interleaved_products_for_file(file_part_index, start_product_id):
    """
    Generates PRODUCTS_PER_FILE products, streaming them to a single output file,
    and recording category/vendor counts.
    """
    start_time = time.time()
    output_path = os.path.join(OUTPUT_DIR, f"products_part_{file_part_index}.json")
    print(f"-> Generating {PRODUCTS_PER_FILE} products for File {file_part_index}...")

    vendor_counts = defaultdict(int)
    category_counts = defaultdict(int)
    
    try:
        with open(output_path, 'w') as f:
            f.write("[\n") # Start JSON array
            
            for i in range(PRODUCTS_PER_FILE):
                global_product_id = start_product_id + i
                product, vendor_id, category_id = generate_product_json(global_product_id)
                
                # Count usage for reporting and base data update
                vendor_counts[vendor_id] += 1
                category_counts[category_id] += 1
                
                # Write the object. Add separator before all objects except the first
                if i > 0:
                    f.write(",\n")
                
                f.write(json.dumps(product))
                
            f.write("\n]") # End JSON array

    except Exception as e:
        print(f"Error writing file {file_part_index}: {e}")
        return None

    duration = time.time() - start_time
    print(f"<- Finished File {file_part_index}. Saved {PRODUCTS_PER_FILE} products in {duration:.2f}s.")
    
    # Return counts to be aggregated in the main thread
    return {
        "file_index": file_part_index,
        "category_counts": dict(category_counts),
        "vendor_counts": dict(vendor_counts)
    }

# --- Base Data and Orchestration ---

def generate_base_data(categories_data, vendors_data):
    """Generates and writes initial categories and vendors data."""
    # 1. Categories
    for i in range(NUM_CATEGORIES):
        categories_data.append({
            "_id": i,
            "label": f"Category Label {i} - {get_random_word()}",
            "products_count": 0 # Will be updated after generation
        })

    # 2. Vendors
    for i in range(VENDOR_COUNT):
        vendors_data.append({
            "_id": i,
            "full_name": f"Vendor Company {i}",
            "products_count": 0 # Will be updated after generation
        })
    
    # Write initial files with zero counts
    with open(CATEGORIES_FILE, 'w') as f:
        json.dump(categories_data, f, indent=2)
    with open(VENDORS_FILE, 'w') as f:
        json.dump(vendors_data, f, indent=2)


def update_base_data_counts(results):
    """Aggregates counts from all processes and updates base data files."""
    
    # Initialize total aggregators
    total_vendor_counts = defaultdict(int)
    total_category_counts = defaultdict(int)

    # 1. Aggregate all counts
    for result in results:
        if result is None: continue
        for cat_id, count in result["category_counts"].items():
            total_category_counts[cat_id] += count
        for ven_id, count in result["vendor_counts"].items():
            total_vendor_counts[ven_id] += count

    # 2. Load existing base data
    with open(CATEGORIES_FILE, 'r') as f:
        categories_data = json.load(f)
    with open(VENDORS_FILE, 'r') as f:
        vendors_data = json.load(f)

    # 3. Update products_count fields
    for cat in categories_data:
        cat_id = cat["_id"]
        cat["products_count"] = total_category_counts[cat_id]
        
    for vendor in vendors_data:
        ven_id = vendor["_id"]
        vendor["products_count"] = total_vendor_counts[ven_id]

    # 4. Write updated base files
    with open(CATEGORIES_FILE, 'w') as f:
        json.dump(categories_data, f, indent=2)
    with open(VENDORS_FILE, 'w') as f:
        json.dump(vendors_data, f, indent=2)
    
    print("\n--- Base Data Counts Updated Successfully ---")


def run_interleaved_generation():
    """Orchestrates the concurrent generation of the four final split files."""
    print(f"\n--- Starting Interleaved Product Generation ---")
    print(f"Total products to generate: {TOTAL_PRODUCTS} (4 files x {PRODUCTS_PER_FILE} each)")
    print(f"Using {MAX_WORKERS} processes...")
    
    # We will launch 4 tasks, one for each final split file
    tasks = []
    current_product_id = 0
    for i in range(1, SPLIT_PARTS + 1):
        tasks.append((i, current_product_id))
        current_product_id += PRODUCTS_PER_FILE

    results = []
    
    # Use ProcessPoolExecutor for true parallel execution
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(generate_interleaved_products_for_file, file_idx, start_id) 
                   for file_idx, start_id in tasks}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Error during file generation: {e}")
                
    return results

# --- Execution ---

if __name__ == '__main__':
    start_time = time.time()
    
    # Setup directories
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate initial base data structure (zero counts)
    categories_data = []
    vendors_data = []
    print("Generating Categories and Vendors structure files...")
    generate_base_data(categories_data, vendors_data)
    
    # 1. Concurrent product generation (writes directly to final split files)
    all_counts = run_interleaved_generation()
    
    # 2. Update base data counts based on generation results
    update_base_data_counts(all_counts)
    
    print("\n--- Generation Complete ---")
    for i in range(1, SPLIT_PARTS + 1):
        print(f"File {i} saved to: {os.path.join(OUTPUT_DIR, f'products_part_{i}.json')}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nTotal execution time: {duration:.2f} seconds")