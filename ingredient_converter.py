import requests
import json
import pandas as pd
from datetime import datetime
import os
import time

class APIKeyManager:
    """Manages multiple API keys with failover logic"""
    
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.exhausted_keys = set()  # Keys that have failed due to quota/auth issues
        self.current_key_index = 0
        
    def get_available_keys(self):
        """Returns list of available (non-exhausted) keys"""
        return [key for i, key in enumerate(self.api_keys) 
                if i not in self.exhausted_keys]
    
    def mark_key_exhausted(self, key_index):
        """Mark a key as exhausted (quota/auth failed)"""
        self.exhausted_keys.add(key_index)
        print(f"⚠️  API key {key_index + 1} marked as exhausted")
    
    def has_available_keys(self):
        """Check if any keys are still available"""
        return len(self.get_available_keys()) > 0

def convert_ingredient_with_failover(key_manager, ingredient_name, source_amount, source_unit, target_unit):
    """
    Convert ingredient measurements using Spoonacular API with automatic key rotation
    
    Args:
        key_manager (APIKeyManager): Manager for API keys
        ingredient_name (str): Name of the ingredient (e.g., "shredded cheese")
        source_amount (float): Amount to convert from
        source_unit (str): Unit to convert from (e.g., "cups")
        target_unit (str): Unit to convert to (e.g., "grams")
    
    Returns:
        tuple: (success: bool, result: dict or None, error_type: str)
    """
    
    # API endpoint
    url = "https://api.spoonacular.com/recipes/convert"
    
    # Try each available API key
    available_keys = key_manager.get_available_keys()
    
    if not available_keys:
        return False, None, "no_keys_available"
    
    for attempt, api_key in enumerate(available_keys):
        # Find the original index of this key
        key_index = key_manager.api_keys.index(api_key)
        
        # Parameters for the API request
        params = {
            'apiKey': api_key,
            'ingredientName': ingredient_name,
            'sourceAmount': source_amount,
            'sourceUnit': source_unit,
            'targetUnit': target_unit
        }
        
        try:
            print(f"  → Trying API key {key_index + 1}/{len(key_manager.api_keys)}")
            
            # Make the GET request
            response = requests.get(url, params=params, timeout=30)
            
            # Check for quota/payment related errors that should exhaust the key
            if response.status_code in [401, 402, 403]:
                print(f"  ✗ API key {key_index + 1}: Authentication/quota error ({response.status_code})")
                key_manager.mark_key_exhausted(key_index)
                continue
            
            # Check for rate limiting
            if response.status_code == 429:
                print(f"  ✗ API key {key_index + 1}: Rate limited (429), trying next key")
                time.sleep(2)  # Wait a bit longer for rate limit
                continue
            
            # Raise for other HTTP errors
            response.raise_for_status()
            
            # Parse JSON response
            result = response.json()
            print(f"  ✓ Success with API key {key_index + 1}")
            return True, result, None
            
        except requests.exceptions.Timeout as e:
            print(f"  ⏱️  Timeout with API key {key_index + 1}: {e}")
            return False, None, "timeout"
            
        except requests.exceptions.ConnectionError as e:
            print(f"  🌐 Connection error with API key {key_index + 1}: {e}")
            return False, None, "connection_error"
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Request error with API key {key_index + 1}: {e}")
            continue
            
        except json.JSONDecodeError as e:
            print(f"  ✗ JSON decode error with API key {key_index + 1}: {e}")
            continue
    
    # All keys tried and failed
    return False, None, "all_keys_failed"

def process_vineet_sheet():
    """
    Process all ingredients from the vineet sheet and add conversion results
    """
    
    # Multiple API keys for failover
    API_KEYS = [
        "10b271a31f244b1bbebcc6addd01213c",
        "229fd521196e445a9d696547348ae7c3",
        "89e5dca8a28d4ea9a45802c369b43e84",
        "dc6d9d6f5a024843bec83eb7d0a12518",
        "f9d9c222e17b4142829ab925041cf413",
        "3e94fab0e7a7491db2711bebf87e7664",
        "5e1cbf5aaa5846fc9616c2c64576b6f2",
        "09af43e637df48dea5fc558af90a96f1"
    ]
    
    # Initialize API key manager
    key_manager = APIKeyManager(API_KEYS)
    
    # File details
    file_path = "spoonconver.xlsx"
    sheet_name = "vineet"
    target_unit = "grams"
    
    try:
        # Read the vineet sheet
        print("Reading data from spoonconver.xlsx, vineet sheet...")
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        print(f"Found {len(df)} ingredients to process")
        print(f"Using {len(API_KEYS)} API keys for failover")
        
        # Prepare new columns for results
        df['converted_amount'] = None
        df['converted_unit'] = None
        df['conversion_status'] = None
        df['api_response'] = None
        df['processed_at'] = None
        
        # Process each ingredient
        for index, row in df.iterrows():
            ingredient_name = row['ingredient_name']
            source_amount = row['amount']
            source_unit = row['unit']
            
            print(f"\nProcessing {index + 1}/{len(df)}: {ingredient_name}")
            print(f"Converting {source_amount} {source_unit} to {target_unit}")
            
            # Skip if essential data is missing
            if pd.isna(ingredient_name) or pd.isna(source_amount) or pd.isna(source_unit):
                print(f"Skipping row {index + 1}: Missing data")
                df.at[index, 'conversion_status'] = 'Skipped - Missing data'
                df.at[index, 'processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                continue
            
            # Check if we have any available API keys left
            if not key_manager.has_available_keys():
                print(f"\n🚫 All API keys exhausted! Stopping process at row {index + 1}")
                print("Saving partial results...")
                
                # Mark remaining rows as not processed
                remaining_rows = df.index[index:]
                for remaining_index in remaining_rows:
                    df.at[remaining_index, 'conversion_status'] = 'Not processed - API keys exhausted'
                    df.at[remaining_index, 'processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                break  # Exit the processing loop
            
            # Make API call with failover
            success, result, error_type = convert_ingredient_with_failover(
                key_manager,
                str(ingredient_name).strip(),
                float(source_amount), 
                str(source_unit).strip(), 
                target_unit
            )
            
            # Process result
            if success and result:
                try:
                    # Extract converted amount and unit
                    converted_amount = result.get('targetAmount', 'N/A')
                    converted_unit = result.get('targetUnit', target_unit)
                    
                    # Update dataframe
                    df.at[index, 'converted_amount'] = converted_amount
                    df.at[index, 'converted_unit'] = converted_unit
                    df.at[index, 'conversion_status'] = 'Success'
                    df.at[index, 'api_response'] = json.dumps(result)
                    df.at[index, 'processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    print(f"✓ Result: {converted_amount} {converted_unit}")
                    
                except Exception as e:
                    print(f"✗ Error processing result: {e}")
                    df.at[index, 'conversion_status'] = f'Error processing result: {e}'
                    df.at[index, 'processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Handle different types of failures
                if error_type == "no_keys_available":
                    print("✗ No API keys available")
                    df.at[index, 'conversion_status'] = 'Failed - No API keys available'
                elif error_type in ["timeout", "connection_error"]:
                    print(f"✗ Network error ({error_type}) - skipping row")
                    df.at[index, 'conversion_status'] = f'Skipped - Network error ({error_type})'
                else:
                    print("✗ API call failed with all keys")
                    df.at[index, 'conversion_status'] = 'Failed - All API keys failed'
                
                df.at[index, 'processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Add delay to respect API rate limits (adjust as needed)
            time.sleep(0.5)  # 0.5 second delay between calls
        
        # Save results back to the same file
        print(f"\nSaving results back to {file_path}...")
        
        # Read all sheets first to preserve them
        with pd.ExcelFile(file_path) as xls:
            all_sheets = {}
            for sheet in xls.sheet_names:
                if sheet == sheet_name:
                    all_sheets[sheet] = df  # Use our updated dataframe
                else:
                    all_sheets[sheet] = pd.read_excel(xls, sheet_name=sheet)
        
        # Write all sheets back
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet, data in all_sheets.items():
                data.to_excel(writer, sheet_name=sheet, index=False)
        
        print("✓ Results saved successfully!")
        
        # Print summary
        success_count = len(df[df['conversion_status'] == 'Success'])
        failed_count = len(df[df['conversion_status'] != 'Success'])
        skipped_network_count = len(df[df['conversion_status'].str.contains('Network error', na=False)])
        not_processed_count = len(df[df['conversion_status'] == 'Not processed - API keys exhausted'])
        
        print(f"\n{'='*50}")
        print("CONVERSION SUMMARY")
        print(f"{'='*50}")
        print(f"Total ingredients: {len(df)}")
        print(f"Successful conversions: {success_count}")
        print(f"Failed conversions: {failed_count}")
        print(f"Skipped (network errors): {skipped_network_count}")
        print(f"Not processed (keys exhausted): {not_processed_count}")
        print(f"API keys exhausted: {len(key_manager.exhausted_keys)}/{len(key_manager.api_keys)}")
        print(f"Results saved to: {file_path} (vineet sheet)")
        
        return df
        
    except FileNotFoundError:
        print(f"Error: File {file_path} not found!")
        return None
    except Exception as e:
        print(f"Error processing file: {e}")
        return None

def main():
    """
    Main function to run the ingredient conversion process
    """
    print("Starting ingredient conversion process...")
    print("Reading from: spoonconver.xlsx (vineet sheet)")
    print("Target unit: grams")
    print("="*50)
    
    # Process the vineet sheet
    result_df = process_vineet_sheet()
    
    if result_df is not None:
        print("\n✓ Process completed successfully!")
        print("Check the spoonconver.xlsx file, vineet sheet for results.")
    else:
        print("\n✗ Process failed!")

if __name__ == "__main__":
    main()