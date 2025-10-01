import requests
import pandas as pd
import string
import time

API_KEY = "dc6d9d6f5a024843bec83eb7d0a12518"  
SEARCH_URL = "https://api.spoonacular.com/food/ingredients/search"
INFO_URL = "https://api.spoonacular.com/food/ingredients/{id}/information"

def fetch_ingredients(query, number=100):
    """Fetch ingredients from Spoonacular by query letter"""
    params = {"query": query, "number": number, "apiKey": API_KEY}
    res = requests.get(SEARCH_URL, params=params)
    res.raise_for_status()
    return res.json().get("results", [])

def fetch_ingredient_info(ingredient_id):
    """Fetch full ingredient details (category, nutrition, etc.)"""
    url = INFO_URL.format(id=ingredient_id)
    params = {"amount": 1, "apiKey": API_KEY}
    res = requests.get(url, params=params)
    res.raise_for_status()
    return res.json()

all_ingredients = []

# Loop A–Z
for letter in string.ascii_lowercase:
    print(f"🔎 Fetching ingredients starting with: {letter}")
    results = fetch_ingredients(letter, number=100)
    
    for item in results:
        try:
            info = fetch_ingredient_info(item["id"])
            all_ingredients.append({
                "id": item["id"],
                "name": item["name"],
                "categoryPath": " > ".join(info.get("categoryPath", [])),
                "image": item.get("image", "")
            })
            time.sleep(0.3)  
        except Exception as e:
            print(f" Skipped {item['name']} - {e}")

df = pd.DataFrame(all_ingredients)
df.drop_duplicates(subset=["id"], inplace=True)
output_file = "ingredients_with_categories.xlsx"
df.to_excel(output_file, index=False)

print(f" Saved {len(df)} ingredients with categories to {output_file}")
