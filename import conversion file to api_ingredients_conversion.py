import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# 1. Database Credentials
DB_INFO = {
    "host": "54.144.89.225",
    "port": "5432",
    "user": "app_user",
    "pass": "App$gr0c",
    "db": "ai_butler_test"
}

TABLE_NAME = "api_ingredients_conversion"
EXCEL_PATH = r"C:\Users\DELL\Desktop\New folder\converted_ingredients5.xlsx"

def upload_to_db():
    try:
        print(f"Reading file from {EXCEL_PATH}...")
        df = pd.read_excel(EXCEL_PATH)

        # 2. Define the Mapping
        column_mapping = {
            'ingredient name': 'ingredientname',
            'source amount': 'source_amount',
            'source unit': 'source_unit',
            'target amount': 'target_amount',
            'target unit': 'target_unit',
            'answer': 'answer',
            'type': 'type'
        }
        
        # 3. FIX: Filter the dataframe to ONLY include the columns we defined above
        # This automatically drops all "Unnamed" columns
        df = df[list(column_mapping.keys())].copy()

        # 4. Rename columns to match DB
        df = df.rename(columns=column_mapping)

        # 5. Add timestamp
        df['created_at'] = datetime.now()

        # 6. Connect and Upload
        engine_url = f"postgresql://{DB_INFO['user']}:{DB_INFO['pass']}@{DB_INFO['host']}:{DB_INFO['port']}/{DB_INFO['db']}"
        engine = create_engine(engine_url)

        # Use index=False so we don't try to upload the row numbers
        df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
        
        print(f"Success! {len(df)} rows uploaded. Extra 'Unnamed' columns were ignored.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    upload_to_db()