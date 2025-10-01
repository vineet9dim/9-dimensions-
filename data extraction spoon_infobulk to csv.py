import requests
import pandas as pd
import json
from typing import List, Dict, Any
from datetime import datetime
import time
import os

def load_ids_from_file(path: str) -> List[int]:
    ids: List[int] = []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.isdigit():
                    ids.append(int(line))
    except Exception as e:
        print(f"Failed to read IDs from {path}: {e}")
    return ids

def fetch_recipe_information(api_key: str, recipe_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Fetch recipe information from Spoonacular API for given recipe IDs (single batch)
    Includes simple retry with exponential backoff for transient errors.
    """
    base_url = "https://api.spoonacular.com/recipes/informationBulk"
    ids_string = ",".join(map(str, recipe_ids))
    params = {
        'apiKey': api_key,
        'ids': ids_string
    }

    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, 'status_code', None)
            if status and (500 <= status < 600 or status in {522, 524}):
                wait = 2 ** attempt
                print(f"Server error {status}. Retry {attempt}/{max_attempts} in {wait}s...")
                time.sleep(wait)
                continue
            else:
                print(f"Error fetching data: {e}")
                return []
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"Network error. Retry {attempt}/{max_attempts} in {wait}s... ({e})")
            time.sleep(wait)
            continue
    print("Max retries reached. Skipping this batch.")
    return []


def fetch_with_fallback(api_key: str, recipe_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Try batch fetch; if it fails or returns empty, fall back to fetching each ID individually.
    """
    results: List[Dict[str, Any]] = []
    batch = fetch_recipe_information(api_key, recipe_ids)
    if batch:
        return batch
    print("Batch failed/empty; falling back to per-ID fetch...")
    for rid in recipe_ids:
        data = fetch_recipe_information(api_key, [rid])
        if data:
            results.extend(data)
        else:
            print(f"  Skipped ID {rid} (no data)")
        # brief delay to be polite
        time.sleep(0.2)
    return results


def extract_ingredient_data(recipes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted_data = []
    for recipe in recipes:
        recipe_id = recipe.get('id')
        title = recipe.get('title', '')
        extended_ingredients = recipe.get('extendedIngredients', [])
        if not extended_ingredients:
            extracted_data.append({
                'recipe_id': recipe_id,
                'recipe_title': title,
                'ingredient_id': '',
                'ingredient_name': '',
                'aisle': '',
                'original': '',
                'amount': '',
                'unit': '',
                'meta': '',
                'measures': ''
            })
        else:
            for ingredient in extended_ingredients:
                extracted_data.append({
                    'recipe_id': recipe_id,
                    'recipe_title': title,
                    'ingredient_id': ingredient.get('id', ''),
                    'aisle': ingredient.get('aisle', ''),
                    'ingredient_name': ingredient.get('name', ''),
                    'original': ingredient.get('original', ''),
                    'amount': ingredient.get('amount', ''),
                    'unit': ingredient.get('unit', ''),
                    'meta': ', '.join(ingredient.get('meta', []) or []),
                    'measures': json.dumps(ingredient.get('measures', {}) or {})
                })
    return extracted_data


def save_to_excel(data: List[Dict[str, Any]], filename: str = 'spoonacular_recipes.xlsx'):
    df = pd.DataFrame(data)
    column_order = [
        'recipe_id', 'recipe_title', 'ingredient_id', 'aisle', 'ingredient_name',
        'original', 'amount', 'unit', 'meta', 'measures'
    ]
    # Ensure columns exist
    df = df.reindex(columns=column_order)

    # Create a unique sheet name per run
    sheet_name = f"Recipe Ingredients {datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        if os.path.exists(filename):
            with pd.ExcelWriter(filename, engine='openpyxl', mode='a') as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                print(f"Appended new sheet '{sheet_name}' to {filename}")
        else:
            with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                print(f"Data saved to {filename} with sheet '{sheet_name}'")
    except PermissionError:
        alt = f"spoonacular_recipes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(alt, engine='openpyxl', mode='w') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        print(f"File was in use. Saved to {alt} with sheet '{sheet_name}'")

    print(f"Total rows: {len(df)}")
    if 'recipe_id' in df.columns:
        print(f"Total recipes: {df['recipe_id'].nunique()}")


def chunk_list(items: List[int], chunk_size: int) -> List[List[int]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def main():
    api_keys = [
        "10b271a31f244b1bbebcc6addd01213c",
    ]
    
    ids_path = r"C:\Users\DELL\Desktop\9dim work\ids_new.txt"

    new_ids = [
        716202, 654125, 649056, 639599, 642230, 641908, 645769, 663169, 649199, 641128,
        636593, 648758, 1096230, 648774, 663074, 650378, 632810, 646123, 652598, 654313,
        644376, 645634, 664575, 1697815, 658641, 658681, 643347, 655003, 643400, 631756,
        636962, 1697573, 636356, 642614, 660842, 644666, 656329, 634891, 654935, 638088,
        643175, 657159, 640395, 642595, 658753, 641270, 660366, 631748, 1046983, 645384,
        638235, 656548, 659109, 642138, 663595, 631863, 648641, 633884, 633133, 636360,
        644859, 641974, 652354, 659906, 648048, 663771, 660292, 644826, 657598, 650871,
        634579
    ]
    try:
        with open(ids_path, "w", encoding="utf-8") as f:
            f.write("\n".join(str(i) for i in new_ids))
        print(f"Wrote {len(new_ids)} ids to {ids_path}")
    except Exception as e:
        print(f"Failed to write ids to {ids_path}: {e}")

    recipe_ids = load_ids_from_file(ids_path)
    if not recipe_ids:
        print("No recipe IDs found in ids_new.txt. Please ensure it contains one ID per line.")
        return
    print(f"Found {len(recipe_ids)} recipe IDs from {ids_path}")

    all_recipes: List[Dict[str, Any]] = []
    for batch in chunk_list(recipe_ids, 15):
        print(f"Fetching batch of {len(batch)} recipes...")
        batch_recipes: List[Dict[str, Any]] = []
        for api_key in api_keys:
            batch_recipes = fetch_with_fallback(api_key, batch)
            if batch_recipes:
                print("  Batch succeeded with current API key.")
                break
            else:
                print("  Current key failed for this batch, trying next key...")
        if batch_recipes:
            all_recipes.extend(batch_recipes)
        else:
            print("  Skipped this batch due to API failures.")

    if not all_recipes:
        print("No recipes fetched. Check API keys/quota.")
        return
    print(f"Successfully fetched {len(all_recipes)} recipes")
    print("Extracting ingredient data...")
    extracted_data = extract_ingredient_data(all_recipes)
    print("Saving to Excel...")
    save_to_excel(extracted_data)
    print("\nSample data (first 3 rows):")
    df_sample = pd.DataFrame(extracted_data[:3])
    print(df_sample.to_string(index=False))


if __name__ == "__main__":
    main()
