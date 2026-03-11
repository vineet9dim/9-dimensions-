import psycopg2
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

API_KEY       = "09af43e637df48dea5fc558af90a96f1"
API_URL       = "https://api.spoonacular.com/recipes/convert"
SOURCE_AMOUNT = 1

# ─── STEP 1: Query DB for ingredients that need conversion ────────────────────
print("Connecting to database...")
conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

print("Fetching ingredients that need conversion...")
cur.execute("""
    SELECT DISTINCT
        ingredient->>'name'                           AS ingredient_name,
        LOWER(TRIM(asu.standard_unit))                AS expected_source_unit,
        asu.target_unit                               AS expected_target_unit,
        asu.is_convertable,
        COUNT(DISTINCT r.id)                          AS recipes_affected
    FROM recipes_normalized r
    JOIN LATERAL jsonb_array_elements(r.updated_extended_ingredients) AS ingredient ON TRUE
    INNER JOIN app_standard_unit asu
        ON LOWER(TRIM(ingredient->'measures'->'metric'->>'unitLong')) = LOWER(TRIM(asu.source_unit))
       AND ingredient->>'consistency' = asu.consistency
    LEFT JOIN api_ingredients_conversion aic
        ON LOWER(TRIM(ingredient->>'name')) = LOWER(TRIM(aic.ingredientname))
       AND asu.standard_unit = aic.source_unit
       AND asu.target_unit   = aic.target_unit
    WHERE asu.is_convertable <> 'NO_CONVERSION'
      AND aic.ingredientname IS NULL
    GROUP BY ingredient->>'name', asu.standard_unit, asu.target_unit, asu.is_convertable
    ORDER BY recipes_affected DESC;
""")

rows = cur.fetchall()
print(f"  Found {len(rows)} ingredient(s) to process.\n")

# ─── STEP 2: Call API & insert results ────────────────────────────────────────
success_count = 0
failed_count  = 0
skipped_count = 0

for row in rows:
    ingredient_name, source_unit, target_unit, is_convertable, recipes_affected = row

    print(f"[{ingredient_name}] | {source_unit} → {target_unit} | recipes: {recipes_affected}")

    params = {
        "ingredientName": ingredient_name,
        "sourceAmount":   SOURCE_AMOUNT,
        "sourceUnit":     source_unit,
        "targetUnit":     target_unit,
        "apiKey":         API_KEY
    }

    try:
        resp = requests.get(API_URL, params=params, timeout=10)
        data = resp.json()

        if resp.status_code == 200 and "targetAmount" in data:
            target_amount = data.get("targetAmount")
            answer        = data.get("answer", "")
            api_status    = "SUCCESS"
            print(f"  ✓ {SOURCE_AMOUNT} {source_unit} = {target_amount} {target_unit}  |  {answer}")
        else:
            target_amount = None
            answer        = data.get("message", str(data))
            api_status    = f"API_ERROR ({resp.status_code})"
            print(f"  ✗ API error: {answer}")

    except Exception as e:
        target_amount = None
        answer        = str(e)
        api_status    = "REQUEST_FAILED"
        print(f"  ✗ Exception: {e}")

    # ── Insert into api_ingredients_conversion ──
    try:
        cur.execute("""
            INSERT INTO api_ingredients_conversion (
                ingredientname,
                source_amount,
                source_unit,
                target_unit,
                target_amount,
                answer,
                api_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ingredientname, source_unit, target_unit)
            DO UPDATE SET
                source_amount = EXCLUDED.source_amount,
                target_amount = EXCLUDED.target_amount,
                answer        = EXCLUDED.answer,
                api_status    = EXCLUDED.api_status;
        """, (
            ingredient_name,
            SOURCE_AMOUNT,
            source_unit,
            target_unit,
            target_amount,
            answer,
            api_status
        ))
        conn.commit()

        if api_status == "SUCCESS":
            success_count += 1
        else:
            failed_count += 1

    except Exception as db_err:
        conn.rollback()
        print(f"  ✗ DB insert failed for '{ingredient_name}': {db_err}")
        skipped_count += 1

    time.sleep(0.25)   # be polite to the API

# ─── Done ─────────────────────────────────────────────────────────────────────
cur.close()
conn.close()
print("\n✅ Done! Database connection closed.")
print(f"   Total processed : {len(rows)}")
print(f"   Successful       : {success_count}")
print(f"   API errors       : {failed_count}")
print(f"   DB insert failed : {skipped_count}")