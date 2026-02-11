import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# 1. Configuration & Paths
EXCEL_PATH = r"C:\Users\DELL\Desktop\New folder\uk_ingredients_for_updated_extended_ingredients.xlsx"

DB_CONFIG = {
    "host": "54.144.89.225",
    "database": "ai_butler_test",
    "user": "app_user",
    "password": "App$gr0c",
    "port": "5432"
}

def process_ingredients():
    # 2. Load Excel Mapping
    try:
        print("Reading Excel file...")
        df = pd.read_excel(EXCEL_PATH)
        df.columns = df.columns.astype(str).str.strip().str.lower()
        
        col_key = 'ingredient name'
        col_val = 'final uk name'
        
        if col_key not in df.columns or col_val not in df.columns:
            print(f"Error: Could not find columns. Found: {list(df.columns)}")
            return

        mapping = {
            str(row[col_key]).strip().lower(): str(row[col_val]).strip()
            for _, row in df.iterrows()
        }
        print(f"Successfully loaded {len(mapping)} ingredient mappings.")

    except Exception as e:
        print(f"Error reading Excel: {e}")
        return

    # 3. Database Operations
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("Fetching data...")
        cursor.execute("""
            SELECT id, extended_ingredients 
            FROM ab_api_ingredients 
            WHERE extended_ingredients IS NOT NULL
        """)
        rows = cursor.fetchall()
        
        print(f"Processing {len(rows)} rows...")
        updated_count = 0

        for row in rows:
            record_id = row['id']
            ingredients = row['extended_ingredients']
            
            if not isinstance(ingredients, list):
                continue
                
            new_ingredients_list = []
            
            for item in ingredients:
                # --- A. Name Mapping ---
                original_name = str(item.get('name', '')).strip().lower()
                if original_name in mapping:
                    uk_name = mapping[original_name]
                    if uk_name.upper() == 'N/A':
                        continue
                    item['name'] = uk_name

                # --- B. Unit & UnitLong Logic ---
                unit_raw = str(item.get('unit', '')).strip()
                unit_lower = unit_raw.lower()
                consistency = str(item.get('consistency', '')).strip().upper()

                # 1. Handle Blank Units -> piece
                if unit_raw == "":
                    item['unit'] = "piece"
                    unit_lower = "piece"
                    if 'measures' in item:
                        if 'us' in item['measures']: item['measures']['us']['unitLong'] = "piece"
                        if 'metric' in item['measures']: item['measures']['metric']['unitLong'] = "piece"
                
                # 2. Handle "pieces" plural
                elif unit_lower == "pieces":
                    if 'measures' in item:
                        if 'us' in item['measures']: item['measures']['us']['unitLong'] = "pieces"
                        if 'metric' in item['measures']: item['measures']['metric']['unitLong'] = "pieces"

                # 3. NEW RULE: piece + SOLID -> unitLong = "piece"
                elif unit_lower == "piece" and consistency == "SOLID":
                    if 'measures' in item:
                        if 'us' in item['measures']: item['measures']['us']['unitLong'] = "piece"
                        if 'metric' in item['measures']: item['measures']['metric']['unitLong'] = "piece"

                # --- C. Consistency & Metric Conversion Logic ---
                metric_data = item.get('measures', {}).get('metric', {})
                metric_unit_long = str(metric_data.get('unitLong', '')).strip().lower()

                # Rule: Cup(s) + SOLID + Milliliters -> Grams
                if unit_lower in ['cup', 'cups'] and consistency == 'SOLID':
                    if metric_unit_long == 'milliliters':
                        item['measures']['metric']['unitLong'] = 'grams'
                        item['measures']['metric']['unitShort'] = 'g'
                        metric_unit_long = 'grams'

                # Rule: Milliliters + SOLID -> Consistency = LIQUID
                if metric_unit_long == 'milliliters' and consistency == 'SOLID':
                    item['consistency'] = 'LIQUID'
                    consistency = 'LIQUID'

                # Rule: piece + LIQUID -> Consistency = SOLID
                if unit_lower == "piece" and consistency == "LIQUID":
                    item['consistency'] = "SOLID"
                
                new_ingredients_list.append(item)

            # 4. Update the Database
            cursor.execute("""
                UPDATE ab_api_ingredients 
                SET updated_extended_ingredients = %s 
                WHERE id = %s
            """, (json.dumps(new_ingredients_list), record_id))
            
            updated_count += 1
            if updated_count % 100 == 0:
                print(f"Progress: {updated_count} rows processed...")

        conn.commit()
        print(f"Finished! Updated {updated_count} records.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Database error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    process_ingredients()