import psycopg2
import psycopg2.extras
import requests
import time

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "54.144.89.225",
    "port":     5432,
    "dbname":   "ai_butler_test",
    "user":     "app_user",
    "password": "App$gr0c"
}

API_KEY        = "09af43e637df48dea5fc558af90a96f1"
API_URL        = "https://api.spoonacular.com/recipes/informationBulk"
BATCH_SIZE     = 100   # Spoonacular allows up to 100 IDs per bulk call

# ─── STEP 1: Fetch source_ids from recipes table ──────────────────────────────
print("Connecting to database...")
conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

print("Fetching source_ids from recipes table...")
cur.execute("""
    SELECT r.id, r.source_id
    FROM recipes r
    WHERE r.source_id IS NOT NULL
      AND r.source_id <> ''
      -- Skip source_ids already present in recipes_normalized
      AND NOT EXISTS (
          SELECT 1
          FROM recipes_normalized rn
          WHERE rn.id = r.source_id::bigint
      )
    ORDER BY r.id;
""")

rows = cur.fetchall()
print(f"  Found {len(rows)} recipe(s) not yet in recipes_normalized.\n")

if not rows:
    print("Nothing to process. Exiting.")
    cur.close()
    conn.close()
    exit()

# Build list of (recipes.id, source_id) pairs
recipe_pairs = [(row[0], row[1]) for row in rows]   # [(internal_id, source_id), ...]

# ─── STEP 2: Process in batches ───────────────────────────────────────────────
total_inserted = 0
total_failed   = 0

def safe_array(value):
    """Return a list or empty list — for ARRAY columns."""
    if isinstance(value, list):
        return value
    return []

def safe_json(value):
    """Return dict/list or None — for JSONB columns."""
    if value is None:
        return None
    return value

def build_record(recipe):
    """Map Spoonacular API response fields to recipes_normalized columns."""
    return {
        "id":                        recipe.get("id"),
        "image":                     recipe.get("image"),
        "image_type":                recipe.get("imageType"),
        "title":                     recipe.get("title"),
        "ready_in_minutes":          recipe.get("readyInMinutes"),
        "servings":                  recipe.get("servings"),
        "source_url":                recipe.get("sourceUrl"),
        "vegetarian":                recipe.get("vegetarian"),
        "vegan":                     recipe.get("vegan"),
        "gluten_free":               recipe.get("glutenFree"),
        "dairy_free":                recipe.get("dairyFree"),
        "very_healthy":              recipe.get("veryHealthy"),
        "cheap":                     recipe.get("cheap"),
        "very_popular":              recipe.get("veryPopular"),
        "sustainable":               recipe.get("sustainable"),
        "low_fodmap":                recipe.get("lowFodmap"),
        "weight_watcher_smart_points": recipe.get("weightWatcherSmartPoints"),
        "gaps":                      recipe.get("gaps"),
        "preparation_minutes":       recipe.get("preparationMinutes"),
        "cooking_minutes":           recipe.get("cookingMinutes"),
        "aggregate_likes":           recipe.get("aggregateLikes"),
        "health_score":              recipe.get("healthScore"),
        "credits_text":              recipe.get("creditsText"),
        "license":                   recipe.get("license"),
        "source_name":               recipe.get("sourceName"),
        "price_per_serving":         recipe.get("pricePerServing"),
        "extended_ingredients":      safe_json(recipe.get("extendedIngredients")),
        "summary":                   recipe.get("summary"),
        "cuisines":                  safe_array(recipe.get("cuisines")),
        "dish_types":                safe_array(recipe.get("dishTypes")),
        "diets":                     safe_array(recipe.get("diets")),
        "occasions":                 safe_array(recipe.get("occasions")),
        "wine_pairing":              safe_json(recipe.get("winePairing")),
        "instructions":              recipe.get("instructions"),
        "analyzed_instructions":     safe_json(recipe.get("analyzedInstructions")),
        "original_id":               recipe.get("originalId"),
        "spoonacular_score":         recipe.get("spoonacularScore"),
        "spoonacular_source_url":    recipe.get("spoonacularSourceUrl"),
    }

INSERT_SQL = """
    INSERT INTO recipes_normalized (
        id, image, image_type, title, ready_in_minutes, servings,
        source_url, vegetarian, vegan, gluten_free, dairy_free,
        very_healthy, cheap, very_popular, sustainable, low_fodmap,
        weight_watcher_smart_points, gaps, preparation_minutes,
        cooking_minutes, aggregate_likes, health_score, credits_text,
        license, source_name, price_per_serving, extended_ingredients,
        summary, cuisines, dish_types, diets, occasions, wine_pairing,
        instructions, analyzed_instructions, original_id,
        spoonacular_score, spoonacular_source_url
    )
    VALUES (
        %(id)s, %(image)s, %(image_type)s, %(title)s, %(ready_in_minutes)s,
        %(servings)s, %(source_url)s, %(vegetarian)s, %(vegan)s,
        %(gluten_free)s, %(dairy_free)s, %(very_healthy)s, %(cheap)s,
        %(very_popular)s, %(sustainable)s, %(low_fodmap)s,
        %(weight_watcher_smart_points)s, %(gaps)s, %(preparation_minutes)s,
        %(cooking_minutes)s, %(aggregate_likes)s, %(health_score)s,
        %(credits_text)s, %(license)s, %(source_name)s, %(price_per_serving)s,
        %(extended_ingredients)s, %(summary)s, %(cuisines)s, %(dish_types)s,
        %(diets)s, %(occasions)s, %(wine_pairing)s, %(instructions)s,
        %(analyzed_instructions)s, %(original_id)s, %(spoonacular_score)s,
        %(spoonacular_source_url)s
    )
    ON CONFLICT (id) DO UPDATE SET
        image                      = EXCLUDED.image,
        image_type                 = EXCLUDED.image_type,
        title                      = EXCLUDED.title,
        ready_in_minutes           = EXCLUDED.ready_in_minutes,
        servings                   = EXCLUDED.servings,
        source_url                 = EXCLUDED.source_url,
        vegetarian                 = EXCLUDED.vegetarian,
        vegan                      = EXCLUDED.vegan,
        gluten_free                = EXCLUDED.gluten_free,
        dairy_free                 = EXCLUDED.dairy_free,
        very_healthy               = EXCLUDED.very_healthy,
        cheap                      = EXCLUDED.cheap,
        very_popular               = EXCLUDED.very_popular,
        sustainable                = EXCLUDED.sustainable,
        low_fodmap                 = EXCLUDED.low_fodmap,
        weight_watcher_smart_points= EXCLUDED.weight_watcher_smart_points,
        gaps                       = EXCLUDED.gaps,
        preparation_minutes        = EXCLUDED.preparation_minutes,
        cooking_minutes            = EXCLUDED.cooking_minutes,
        aggregate_likes            = EXCLUDED.aggregate_likes,
        health_score               = EXCLUDED.health_score,
        credits_text               = EXCLUDED.credits_text,
        license                    = EXCLUDED.license,
        source_name                = EXCLUDED.source_name,
        price_per_serving          = EXCLUDED.price_per_serving,
        extended_ingredients       = EXCLUDED.extended_ingredients,
        summary                    = EXCLUDED.summary,
        cuisines                   = EXCLUDED.cuisines,
        dish_types                 = EXCLUDED.dish_types,
        diets                      = EXCLUDED.diets,
        occasions                  = EXCLUDED.occasions,
        wine_pairing               = EXCLUDED.wine_pairing,
        instructions               = EXCLUDED.instructions,
        analyzed_instructions      = EXCLUDED.analyzed_instructions,
        original_id                = EXCLUDED.original_id,
        spoonacular_score          = EXCLUDED.spoonacular_score,
        spoonacular_source_url     = EXCLUDED.spoonacular_source_url;
"""

# ─── Batch processing loop ─────────────────────────────────────────────────────
for batch_start in range(0, len(recipe_pairs), BATCH_SIZE):
    batch        = recipe_pairs[batch_start : batch_start + BATCH_SIZE]
    source_ids   = [pair[1] for pair in batch]
    ids_str      = ",".join(source_ids)

    batch_num    = (batch_start // BATCH_SIZE) + 1
    total_batches = (len(recipe_pairs) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Batch {batch_num}/{total_batches} — requesting {len(batch)} recipe(s)...")

    try:
        resp = requests.get(
            API_URL,
            params={
                "ids":              ids_str,
                "includeNutrition": "false",
                "apiKey":           API_KEY
            },
            timeout=30
        )

        if resp.status_code != 200:
            print(f"  ✗ API error {resp.status_code}: {resp.text[:200]}")
            total_failed += len(batch)
            time.sleep(1)
            continue

        api_recipes = resp.json()   # list of recipe objects
        print(f"  API returned {len(api_recipes)} recipe(s).")

        inserted_in_batch = 0
        for recipe in api_recipes:
            try:
                record = build_record(recipe)

                # Cast JSONB fields to JSON string for psycopg2
                import json
                for jsonb_col in ("extended_ingredients", "wine_pairing",
                                  "analyzed_instructions"):
                    if record[jsonb_col] is not None:
                        record[jsonb_col] = json.dumps(record[jsonb_col])

                cur.execute(INSERT_SQL, record)
                inserted_in_batch += 1
                print(f"    ✓ Inserted/updated recipe id={recipe.get('id')} — {recipe.get('title')}")

            except Exception as insert_err:
                conn.rollback()
                print(f"    ✗ Insert failed for recipe id={recipe.get('id')}: {insert_err}")
                total_failed += 1
                continue

        conn.commit()
        total_inserted += inserted_in_batch
        print(f"  Batch {batch_num} committed — {inserted_in_batch} record(s) saved.\n")

    except Exception as req_err:
        print(f"  ✗ Request failed for batch {batch_num}: {req_err}")
        total_failed += len(batch)

    time.sleep(0.5)   # be polite to the API

# ─── Done ─────────────────────────────────────────────────────────────────────
cur.close()
conn.close()
print("=" * 55)
print("✅ Done! Database connection closed.")
print(f"   Total recipes fetched from DB : {len(recipe_pairs)}")
print(f"   Successfully inserted/updated : {total_inserted}")
print(f"   Failed                        : {total_failed}")