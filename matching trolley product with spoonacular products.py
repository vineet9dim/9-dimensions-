import pandas as pd
import re
from datetime import datetime
import os

def match_ingredients_to_products(ingredient_file, product_file, output_file):
    """
    Match ingredients to products based on Aisle_ID and ingredient name in product name.
    
    FEATURES:
    1. Splits ingredients with "/" and searches for each part separately
    2. Removes text in parentheses before searching
    
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
        required_ingredient_cols = ['ingredient name', 'main id', 'secondary id']
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
        ingredients_df['ingredient name'] = ingredients_df['ingredient name'].fillna('').astype(str).str.strip()
        products_df['product_name'] = products_df['product_name'].fillna('').astype(str).str.strip()
        products_df['product_code'] = products_df['product_code'].fillna('').astype(str)
        products_df['store'] = products_df['store'].fillna('').astype(str)
        
        # Remove empty ingredients
        ingredients_df = ingredients_df[ingredients_df['ingredient name'] != '']
        print(f"   ✓ Data cleaned")
        
        # Group products by aisle_id for faster lookup
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Optimizing product lookup...")
        products_by_aisle = products_df.groupby('aisle_id')
        print(f"   ✓ Products grouped by {len(products_by_aisle)} aisles")
        
        # Prepare the output list
        results = []
        total_matches = 0
        ingredients_with_slash = 0
        ingredients_with_parentheses = 0
        
        # Iterate through each ingredient
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Matching ingredients to products...")
        print("-" * 60)
        
        for idx, ingredient_row in ingredients_df.iterrows():
            original_ingredient_name = ingredient_row['ingredient name'].strip()
            main_id = ingredient_row['main id']
            secondary_id = ingredient_row['secondary id']
            
            # Skip if ingredient name is empty
            if not original_ingredient_name:
                continue
            
            # STEP 1: Remove text in parentheses
            # Example: "Sun-dried Tomatoes (in oil)" -> "Sun-dried Tomatoes"
            ingredient_cleaned = re.sub(r'\([^)]*\)', '', original_ingredient_name).strip()
            
            if '(' in original_ingredient_name:
                ingredients_with_parentheses += 1
            
            # STEP 2: Check if ingredient contains "/" and split if needed
            # Example: "Flaked almonds / Chopped almonds" -> ["Flaked almonds", "Chopped almonds"]
            if '/' in ingredient_cleaned:
                ingredients_with_slash += 1
                ingredient_parts = [part.strip() for part in ingredient_cleaned.split('/') if part.strip()]
            else:
                ingredient_parts = [ingredient_cleaned]
            
            matched_products = []
            search_terms_used = []
            
            # STEP 3: Search for EACH ingredient part
            for ingredient_part in ingredient_parts:
                if not ingredient_part:
                    continue
                    
                ingredient_name_lower = ingredient_part.lower()
                search_terms_used.append(ingredient_part)
                
                # Use secondary aisle ID as per requirement
                # If secondary ID doesn't exist, fall back to main ID
                aisle_ids_to_search = []
                if pd.notna(secondary_id):
                    aisle_ids_to_search.append(secondary_id)
                elif pd.notna(main_id):
                    aisle_ids_to_search.append(main_id)
                
                # Search in the specified aisles for THIS ingredient part
                for aisle_id in aisle_ids_to_search:
                    try:
                        filtered_products = products_by_aisle.get_group(aisle_id)
                        
                        # Search for this ingredient part in product names
                        for _, product_row in filtered_products.iterrows():
                            product_name_lower = product_row['product_name'].lower()
                            
                            # Check if ingredient name is in product name (partial match, case-insensitive)
                            # Use word boundary to avoid matching "gin" in "ginger"
                            pattern = r'\b' + re.escape(ingredient_name_lower) + r'\b'
                            
                            if re.search(pattern, product_name_lower):
                                # Check if this product is already matched (to avoid duplicates)
                                product_key = (product_row['product_code'], product_row['store'])
                                if not any(p['product_code'] == product_row['product_code'] and 
                                         p['store'] == product_row['store'] for p in matched_products):
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
            
            # Create search terms string
            search_terms_str = ' OR '.join([f'"{term}"' for term in search_terms_used])
            
            if matched_products:
                # Separate by store
                product_codes = ' | '.join([f"{p['product_code']}" for p in matched_products])
                product_names = ' | '.join([p['product_name'] for p in matched_products])
                stores = ' | '.join([p['store'] for p in matched_products])
                
                # Get unique stores
                unique_stores = ', '.join(sorted(set([p['store'] for p in matched_products])))
                
                results.append({
                    'Original_Ingredient': original_ingredient_name,
                    'Search_Terms_Used': search_terms_str,
                    'Main_Aisle_ID': main_id,
                    'Secondary_Aisle_ID': secondary_id,
                    'Match_Count': match_count,
                    'Unique_Stores': unique_stores,
                    'Product_Codes': product_codes,
                    'Stores': stores,
                    'Product_Names': product_names
                })
            else:
                # No matches found
                results.append({
                    'Original_Ingredient': original_ingredient_name,
                    'Search_Terms_Used': search_terms_str,
                    'Main_Aisle_ID': main_id,
                    'Secondary_Aisle_ID': secondary_id,
                    'Match_Count': 0,
                    'Unique_Stores': '',
                    'Product_Codes': 'No matches found',
                    'Stores': '',
                    'Product_Names': 'No matches found'
                })
            
            # Progress indicator
            if (idx + 1) % 100 == 0 or (idx + 1) == len(ingredients_df):
                progress = (idx + 1) / len(ingredients_df) * 100
                print(f"   Progress: {idx + 1}/{len(ingredients_df)} ({progress:.1f}%) - Total matches: {total_matches}")
        
        print("-" * 60)
        
        # Create output DataFrame
        output_df = pd.DataFrame(results)
        
        # Sort by match count (descending) and then by ingredient name
        output_df = output_df.sort_values(['Match_Count', 'Original_Ingredient'], ascending=[False, True])
        
        # Save to Excel with formatting
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Saving results to {output_file}...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Main results
            output_df.to_excel(writer, sheet_name='Matched Results', index=False)
            
            # Summary statistics
            summary_data = {
                'Metric': [
                    'Total Ingredients Processed',
                    'Ingredients with "/" (Slash)',
                    'Ingredients with "()" (Parentheses)',
                    'Ingredients with Matches',
                    'Ingredients without Matches',
                    'Total Product Matches Found',
                    'Average Matches per Ingredient',
                    'Match Rate (%)',
                    'Processing Date'
                ],
                'Value': [
                    len(ingredients_df),
                    ingredients_with_slash,
                    ingredients_with_parentheses,
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
            top_matches = output_df[output_df['Match_Count'] > 0].head(20)[['Original_Ingredient', 'Search_Terms_Used', 'Match_Count', 'Unique_Stores']]
            top_matches.to_excel(writer, sheet_name='Top 20 Matches', index=False)
            
            # Unmatched ingredients
            unmatched = output_df[output_df['Match_Count'] == 0][['Original_Ingredient', 'Search_Terms_Used', 'Main_Aisle_ID', 'Secondary_Aisle_ID']]
            unmatched.to_excel(writer, sheet_name='Unmatched Ingredients', index=False)
            
            # Ingredients with slashes (for review)
            slash_ingredients = output_df[output_df['Search_Terms_Used'].str.contains(' OR ')][['Original_Ingredient', 'Search_Terms_Used', 'Match_Count', 'Unique_Stores']]
            if len(slash_ingredients) > 0:
                slash_ingredients.to_excel(writer, sheet_name='Slash Ingredients', index=False)
            
        print(f"   ✓ Results saved successfully!")
        
        # Print summary
        print("\n" + "="*60)
        print("SUMMARY REPORT")
        print("="*60)
        print(f"Total ingredients processed:      {len(ingredients_df)}")
        print(f"Ingredients with '/' (slash):     {ingredients_with_slash}")
        print(f"Ingredients with '()' (parens):   {ingredients_with_parentheses}")
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
                print(f"{row['Original_Ingredient']:35} → {row['Match_Count']} matches")
                print(f"{'':35}   Searched: {row['Search_Terms_Used']}")
            print("="*60)
        
        # Show some slash ingredient examples
        slash_examples = output_df[output_df['Search_Terms_Used'].str.contains(' OR ')].head(5)
        if len(slash_examples) > 0:
            print("\n" + "="*60)
            print("SAMPLE SLASH INGREDIENTS (Split Search)")
            print("="*60)
            for _, row in slash_examples.iterrows():
                print(f"\nOriginal: {row['Original_Ingredient']}")
                print(f"Searched: {row['Search_Terms_Used']}")
                print(f"Matches:  {row['Match_Count']}")
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
    ingredient_file = r"C:\Users\DELL\Desktop\aisle matching\grok_spn_aisle1.xlsx"
    product_file = r"C:\Users\DELL\Desktop\aisle matching\output_with_aisle_ids1.xlsx"
    output_file = r"C:\Users\DELL\Desktop\aisle matching\ingredient_product_matches2.xlsx"
    
    # Run the matching
    results = match_ingredients_to_products(ingredient_file, product_file, output_file)
    
    # Display sample results if successful
    if results is not None and len(results) > 0:
        print("\n" + "="*60)
        print("SAMPLE RESULTS (First 5 with matches)")
        print("="*60)
        sample = results[results['Match_Count'] > 0].head(5)
        for idx, row in sample.iterrows():
            print(f"\nOriginal Ingredient: {row['Original_Ingredient']}")
            print(f"  Search Terms Used: {row['Search_Terms_Used']}")
            print(f"  Secondary Aisle ID: {row['Secondary_Aisle_ID']}")
            print(f"  Matches: {row['Match_Count']}")
            print(f"  Stores: {row['Unique_Stores']}")
            if row['Match_Count'] <= 3:
                print(f"  Products: {row['Product_Names'][:200]}...")
        print("="*60)