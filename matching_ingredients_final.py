import pandas as pd
import re
from datetime import datetime
import os

def match_ingredients_to_products(ingredient_file, product_file, output_file):
    """
    Match ingredients to products based on Aisle_ID and ingredient name in product name.
    
    Parameters:
    - ingredient_file: Path to the ingredient Excel file (grok_spn_aisle.xlsx)
    - product_file: Path to the product Excel file (output_with_aisle_ids.xlsx)
    - output_file: Path for the output Excel file
    """
    
    try:
        # Read the Excel files
        print("="*60)
        print("INGREDIENT TO PRODUCT MATCHER")
        print("="*60)
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Reading ingredient file...")
        ingredients_df = pd.read_excel(ingredient_file)
        print(f"   ✓ Loaded {len(ingredients_df)} ingredients")
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Reading product file...")
        products_df = pd.read_excel(product_file)
        print(f"   ✓ Loaded {len(products_df)} products")
        
        # Validate required columns
        required_ingredient_cols = ['Ingredient', 'BC2', 'Aisle_ID']
        required_product_cols = ['product_code', 'product_name', 'aisle_id', 'store']
        
        missing_ing_cols = [col for col in required_ingredient_cols if col not in ingredients_df.columns]
        missing_prod_cols = [col for col in required_product_cols if col not in products_df.columns]
        
        if missing_ing_cols:
            print(f"\n❌ ERROR: Missing columns in ingredient file: {missing_ing_cols}")
            print(f"   Available columns: {ingredients_df.columns.tolist()}")
            return None
            
        if missing_prod_cols:
            print(f"\n❌ ERROR: Missing columns in product file: {missing_prod_cols}")
            print(f"   Available columns: {products_df.columns.tolist()}")
            return None
        
        # Clean data
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Cleaning data...")
        ingredients_df['Ingredient'] = ingredients_df['Ingredient'].fillna('').astype(str).str.strip()
        products_df['product_name'] = products_df['product_name'].fillna('').astype(str).str.strip()
        products_df['product_code'] = products_df['product_code'].fillna('').astype(str)
        products_df['store'] = products_df['store'].fillna('').astype(str)
        
        # Remove empty ingredients
        ingredients_df = ingredients_df[ingredients_df['Ingredient'] != '']
        print(f"   ✓ Data cleaned")
        
        # Group products by aisle_id for faster lookup
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Optimizing product lookup...")
        products_by_aisle = products_df.groupby('aisle_id')
        print(f"   ✓ Products grouped by {len(products_by_aisle)} aisles")
        
        # Prepare the output list
        results = []
        total_matches = 0
        
        # Iterate through each ingredient
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Matching ingredients to products...")
        print("-" * 60)
        
        for idx, ingredient_row in ingredients_df.iterrows():
            ingredient_name = ingredient_row['Ingredient'].lower().strip()
            aisle_id = ingredient_row['Aisle_ID']
            aisle_name = ingredient_row['BC2']  # This is the aisle name column
            
            # Skip if ingredient name is empty
            if not ingredient_name:
                continue
            
            matched_products = []
            
            # Get products for this aisle_id
            try:
                filtered_products = products_by_aisle.get_group(aisle_id)
                
                # Vectorized matching for better performance
                for _, product_row in filtered_products.iterrows():
                    product_name_lower = product_row['product_name'].lower()
                    
                    # Check if ingredient name is in product name (partial match, case-insensitive)
                    # Use word boundary to avoid matching "gin" in "ginger"
                    pattern = r'\b' + re.escape(ingredient_name) + r'\b'
                    
                    if re.search(pattern, product_name_lower):
                        matched_products.append({
                            'product_code': product_row['product_code'],
                            'product_name': product_row['product_name'],
                            'store': product_row['store']
                        })
                        
            except KeyError:
                # No products found for this aisle_id
                pass
            
            # Create result entry
            match_count = len(matched_products)
            total_matches += match_count
            
            if matched_products:
                # Separate by store
                product_codes = ' | '.join([f"{p['product_code']}" for p in matched_products])
                product_names = ' | '.join([p['product_name'] for p in matched_products])
                stores = ' | '.join([p['store'] for p in matched_products])
                
                # Get unique stores
                unique_stores = ', '.join(sorted(set([p['store'] for p in matched_products])))
                
                results.append({
                    'Ingredient': ingredient_row['Ingredient'],
                    'Aisle_Name': aisle_name,
                    'Aisle_ID': aisle_id,
                    'Match_Count': match_count,
                    'Unique_Stores': unique_stores,
                    'Product_Codes': product_codes,
                    'Stores': stores,
                    'Product_Names': product_names
                })
            else:
                # No matches found
                results.append({
                    'Ingredient': ingredient_row['Ingredient'],
                    'Aisle_Name': aisle_name,
                    'Aisle_ID': aisle_id,
                    'Match_Count': 0,
                    'Unique_Stores': '',
                    'Product_Codes': 'No matches found',
                    'Stores': '',
                    'Product_Names': 'No matches found'
                })
            
            # Progress indicator
            if (idx + 1) % 10 == 0 or (idx + 1) == len(ingredients_df):
                progress = (idx + 1) / len(ingredients_df) * 100
                print(f"   Progress: {idx + 1}/{len(ingredients_df)} ({progress:.1f}%) - Total matches so far: {total_matches}")
        
        print("-" * 60)
        
        # Create output DataFrame
        output_df = pd.DataFrame(results)
        
        # Sort by match count (descending) and then by ingredient name
        output_df = output_df.sort_values(['Match_Count', 'Ingredient'], ascending=[False, True])
        
        # Save to Excel with formatting
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Saving results to {output_file}...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Main results
            output_df.to_excel(writer, sheet_name='Matched Results', index=False)
            
            # Summary statistics
            summary_data = {
                'Metric': [
                    'Total Ingredients Processed',
                    'Ingredients with Matches',
                    'Ingredients without Matches',
                    'Total Product Matches Found',
                    'Average Matches per Ingredient',
                    'Match Rate (%)',
                    'Processing Date'
                ],
                'Value': [
                    len(ingredients_df),
                    len(output_df[output_df['Match_Count'] > 0]),
                    len(output_df[output_df['Match_Count'] == 0]),
                    output_df['Match_Count'].sum(),
                    f"{output_df['Match_Count'].mean():.2f}",
                    f"{(len(output_df[output_df['Match_Count'] > 0]) / len(ingredients_df) * 100):.1f}%",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Top matched ingredients
            top_matches = output_df[output_df['Match_Count'] > 0].head(20)[['Ingredient', 'Match_Count', 'Unique_Stores']]
            top_matches.to_excel(writer, sheet_name='Top 20 Matches', index=False)
            
        print(f"   ✓ Results saved successfully!")
        
        # Print summary
        print("\n" + "="*60)
        print("SUMMARY REPORT")
        print("="*60)
        print(f"Total ingredients processed:      {len(ingredients_df)}")
        print(f"Ingredients with matches:         {len(output_df[output_df['Match_Count'] > 0])}")
        print(f"Ingredients without matches:      {len(output_df[output_df['Match_Count'] == 0])}")
        print(f"Total product matches found:      {output_df['Match_Count'].sum()}")
        print(f"Average matches per ingredient:   {output_df['Match_Count'].mean():.2f}")
        print(f"Match rate:                       {(len(output_df[output_df['Match_Count'] > 0]) / len(ingredients_df) * 100):.1f}%")
        print(f"\nOutput file:                      {os.path.abspath(output_file)}")
        print("="*60)
        
        # Show top matches
        if len(output_df[output_df['Match_Count'] > 0]) > 0:
            print("\n" + "="*60)
            print("TOP 10 INGREDIENTS WITH MOST MATCHES")
            print("="*60)
            top_10 = output_df[output_df['Match_Count'] > 0].head(10)
            for _, row in top_10.iterrows():
                print(f"{row['Ingredient']:30} → {row['Match_Count']} matches ({row['Unique_Stores']})")
            print("="*60)
        
        return output_df
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: File not found - {e}")
        return None
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None


# Example usage:
if __name__ == "__main__":
    ingredient_file = r"C:\Users\DELL\Desktop\aisle matching\grok_spn_aisle.xlsx"
    product_file = r"C:\Users\DELL\Desktop\aisle matching\output_with_aisle_ids1.xlsx"
    output_file = r"C:\Users\DELL\Desktop\aisle matching\ingredient_product_matches.xlsx"
    
    # Run the matching
    results = match_ingredients_to_products(ingredient_file, product_file, output_file)
    
    # Display sample results if successful
    if results is not None and len(results) > 0:
        print("\n" + "="*60)
        print("SAMPLE RESULTS (First 5 with matches)")
        print("="*60)
        sample = results[results['Match_Count'] > 0].head(5)
        for idx, row in sample.iterrows():
            print(f"\nIngredient: {row['Ingredient']}")
            print(f"  Aisle: {row['Aisle_Name']} (ID: {row['Aisle_ID']})")
            print(f"  Matches: {row['Match_Count']}")
            print(f"  Stores: {row['Unique_Stores']}")
            if row['Match_Count'] <= 3:
                print(f"  Products: {row['Product_Names'][:200]}...")
        print("="*60)