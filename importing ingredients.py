import requests
import pandas as pd
import string

API_KEY = "229fd521196e445a9d696547348ae7c3"
BASE_URL = "https://api.spoonacular.com/food/ingredients/search"

def fetch_ingredients(query, number=200):
    """Fetch ingredients from Spoonacular by query letter"""
    params = {
        "query": query,
        "number": number, 
        "apiKey": API_KEY
    }
    res = requests.get(BASE_URL, params=params)
    res.raise_for_status()
    return res.json().get("results", [])

all_ingredients = []
for letter in string.ascii_lowercase:
    print(f"Fetching ingredients starting with: {letter}")
    results = fetch_ingredients(letter, number=200)
    for item in results:
        all_ingredients.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "image": item.get("image")
        })

df = pd.DataFrame(all_ingredients)
df.drop_duplicates(subset=["id"], inplace=True)
output_file = "final_ingredients_a_to_z.xlsx"
df.to_excel(output_file, index=False)
print(" Saved ")
