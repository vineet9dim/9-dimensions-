import pandas as pd
import requests
import time

FILE_PATH = r"C:\Users\DELL\Desktop\New folder\ingredient_conversion(11-02-2026).csv"
OUTPUT_PATH = r"C:\Users\DELL\Desktop\New folder\converted_ingredients6.xlsx"
ENDPOINT = "https://api.spoonacular.com/recipes/convert"

API_KEYS = [
    "10b271a31f244b1bbebcc6addd01213c",
    "229fd521196e445a9d696547348ae7c3",
    "89e5dca8a28d4ea9a45802c369b43e84",
    "dc6d9d6f5a024843bec83eb7d0a12518",
    "5e1cbf5aaa5846fc9616c2c64576b6f2",
    "09af43e637df48dea5fc558af90a96f1",
    "9dc4cba0f7af4e52bcda2038472a6eae",
    "68e46665c9e841e889e096bdad6aa4b0",
    "da23f1a6e3364155ac5a4aded2b07bfa",
    "5b9359356a114f2db6e005d75f54ee25",
    "ce77315063a5478da3670b5c06c7e17c",
    "af1133b7cfa5443fa5918691a7caa624",
    "3803850ebd8b4c89a06e5b5e867ea305"

]

def get_conversion():
    df = pd.read_csv(FILE_PATH)
    
    results = []
    key_index = 0

    for index, row in df.iterrows():
        success = False
        
        while not success and key_index < len(API_KEYS):
            params = {
                "ingredientName": row['ingredientname'],
                "sourceAmount": row['sourceamount'],
                "sourceUnit": row['sourceunit'],
                "targetUnit": row['targetunit'],
                "apiKey": API_KEYS[key_index]
            }

            try:
                response = requests.get(ENDPOINT, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    results.append({
                        "ingredient name": row['ingredientname'],
                        "source amount": data.get("sourceAmount"),
                        "source unit": data.get("sourceUnit"),
                        "target amount": data.get("targetAmount"),
                        "target unit": data.get("targetUnit"),
                        "answer": data.get("answer"),
                        "type": data.get("type")
                    })
                    print(f"Row {index+1}: Success")
                    success = True
                
                elif response.status_code in [402, 429]:
                    print(f"API Key {key_index + 1} exhausted. Switching keys...")
                    key_index += 1
                
                else:
                    print(f"Row {index+1}: Error {response.status_code} - {response.text}")
                    results.append({"ingredient name": row['ingredientname'], "answer": "Error/Not Found"})
                    success = True 
                    
            except Exception as e:
                print(f"Connection error: {e}")
                time.sleep(2)
        
        if key_index >= len(API_KEYS):
            print("All API keys exhausted. Stopping.")
            break

    output_df = pd.DataFrame(results)
    output_df.to_excel(OUTPUT_PATH, index=False)
    print(f"Processing complete. File saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    get_conversion()