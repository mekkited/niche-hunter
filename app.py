import os
import pandas as pd
import random
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
import re # For parsing competing products

# Initialize Flask App
app = Flask(__name__)
CORS(app) # Enable CORS for all routes

# Global variable to hold the loaded niche data
niche_data_df = None
CSV_FILE_NAME = 'US_AMAZON_magnet__2025-06-05.csv' # Make sure this matches your uploaded CSV name

def parse_competing_products(value):
    """
    Cleans and converts the 'Competing Products' column to a numeric type.
    Handles strings like ">1,000" or "826".
    """
    if pd.isna(value):
        return 0 # Treat missing values as 0 or some other indicator of no data
    s = str(value).replace(',', '').replace('>', '').strip()
    if s.lower() == 'n/a' or not s:
        return 0
    try:
        return int(s)
    except ValueError:
        return 0 # Or handle error appropriately, e.g., by logging

def load_data():
    """
    Loads and preprocesses data from the CSV file.
    This function will be called once when the Flask app starts.
    """
    global niche_data_df
    try:
        # Check if the CSV file exists
        if not os.path.exists(CSV_FILE_NAME):
            print(f"CRITICAL ERROR: CSV file '{CSV_FILE_NAME}' not found. Please upload it to the project root.")
            niche_data_df = pd.DataFrame() # Use an empty DataFrame
            return

        df = pd.read_csv(CSV_FILE_NAME)
        print(f"Successfully loaded {CSV_FILE_NAME} with {len(df)} rows.")

        # Ensure required columns exist
        required_columns = ['Keyword Phrase', 'Search Volume', 'Competing Products', 'category']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"CRITICAL ERROR: The CSV file is missing required columns: {', '.join(missing_columns)}")
            print("Please ensure your CSV has 'Keyword Phrase', 'Search Volume', 'Competing Products', and a 'category' column.")
            niche_data_df = pd.DataFrame()
            return

        # Rename columns for easier use, and handle potential NaN in 'Search Volume'
        df.rename(columns={
            'Keyword Phrase': 'name',
            'Search Volume': 'search_volume_numeric', # Keep original numeric search volume
            'Competing Products': 'amazon_results_raw'
        }, inplace=True)
        
        # Clean 'amazon_results_raw' and convert to numeric 'amazon_results'
        df['amazon_results'] = df['amazon_results_raw'].apply(parse_competing_products)

        # Convert 'search_volume_numeric' to numeric, coercing errors to NaN, then fill NaN with 0
        df['search_volume_numeric'] = pd.to_numeric(df['search_volume_numeric'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)


        # Assign 'searchVolumeText' based on numeric 'search_volume_numeric'
        df['searchVolumeText'] = df['search_volume_numeric'].apply(lambda x: 'High' if x > 100 else 'Low')
        
        # Ensure 'category' column is treated as string
        df['category'] = df['category'].astype(str).str.lower().str.strip()

        niche_data_df = df
        print("Data loaded and preprocessed successfully.")
        print(f"Sample of processed data:\n{niche_data_df.head()}")
        print(f"Data types:\n{niche_data_df.dtypes}")


    except FileNotFoundError:
        print(f"CRITICAL ERROR: CSV file '{CSV_FILE_NAME}' not found during load_data().")
        niche_data_df = pd.DataFrame() # Initialize with an empty DataFrame
    except Exception as e:
        print(f"CRITICAL ERROR loading data: {e}")
        niche_data_df = pd.DataFrame() # Initialize with an empty DataFrame

# Load data when the application starts
load_data()

@app.route('/api/trends', methods=['GET'])
def get_trends():
    if niche_data_df is None or niche_data_df.empty:
        print("Warning: Niche data is not loaded or is empty. Returning empty list.")
        return jsonify([])

    book_category_filter = request.args.get('bookType', 'all').lower().strip()
    
    # 1. Filter by category (if not 'all')
    if book_category_filter != 'all':
        filtered_df = niche_data_df[niche_data_df['category'] == book_category_filter].copy()
    else:
        filtered_df = niche_data_df.copy()

    if filtered_df.empty:
        print(f"No data found for category: {book_category_filter}")
        return jsonify([])
        
    # 2. Filter for Amazon results < 1000
    low_comp_df = filtered_df[filtered_df['amazon_results'] < 1000].copy()

    if low_comp_df.empty:
        print(f"No low competition niches (<1000 results) found for category: {book_category_filter}")
        return jsonify([])

    # 3. Separate into <500 results and 500-999 results
    ultra_low_comp_df = low_comp_df[low_comp_df['amazon_results'] < 500].copy()
    medium_low_comp_df = low_comp_df[(low_comp_df['amazon_results'] >= 500) & (low_comp_df['amazon_results'] < 1000)].copy()

    # Sort to try and get somewhat consistent starting points for weekly rotation
    # Sorting by search volume descending, then by name to have a stable order
    ultra_low_comp_df = ultra_low_comp_df.sort_values(by=['search_volume_numeric', 'name'], ascending=[False, True])
    medium_low_comp_df = medium_low_comp_df.sort_values(by=['search_volume_numeric', 'name'], ascending=[False, True])

    # 4. Weekly Rotation Logic
    current_week = datetime.now().isocalendar()[1] # Get current week number (1-53)
    
    selected_niches = []
    
    # Select up to 15 from ultra-low competition (<500)
    num_ultra_low = len(ultra_low_comp_df)
    if num_ultra_low > 0:
        start_index_ultra = ((current_week - 1) * 15) % num_ultra_low if num_ultra_low > 0 else 0
        end_index_ultra = start_index_ultra + 15
        
        # Handle wrapping around the list if necessary
        if end_index_ultra > num_ultra_low:
            selected_ultra_low = pd.concat([ultra_low_comp_df.iloc[start_index_ultra:], ultra_low_comp_df.iloc[:end_index_ultra-num_ultra_low]])
        else:
            selected_ultra_low = ultra_low_comp_df.iloc[start_index_ultra:end_index_ultra]
        selected_niches.extend(selected_ultra_low.to_dict('records'))

    # Select remaining (up to 5) from medium-low competition (500-999) to reach ~20 total
    remaining_needed = 20 - len(selected_niches)
    num_medium_low = len(medium_low_comp_df)

    if remaining_needed > 0 and num_medium_low > 0:
        start_index_medium = ((current_week - 1) * 5) % num_medium_low if num_medium_low > 0 else 0 # Different offset logic for this smaller pool
        end_index_medium = start_index_medium + remaining_needed
        
        if end_index_medium > num_medium_low:
            selected_medium_low = pd.concat([medium_low_comp_df.iloc[start_index_medium:], medium_low_comp_df.iloc[:end_index_medium-num_medium_low]])
        else:
            selected_medium_low = medium_low_comp_df.iloc[start_index_medium:end_index_medium]
        selected_niches.extend(selected_medium_low.to_dict('records'))
    
    # Ensure we don't exceed 20 niches due to concat logic if both lists are small
    final_selected_niches = selected_niches[:20]

    # Prepare for JSON output (select only necessary columns)
    output_niches = []
    for niche in final_selected_niches:
        output_niches.append({
            'name': niche.get('name'),
            'searchVolumeText': niche.get('searchVolumeText'), # Already calculated
            'amazonResults': int(niche.get('amazon_results')), # Ensure it's int
            'category': niche.get('category')
        })
    
    print(f"Returning {len(output_niches)} niches for category '{book_category_filter}', week {current_week}.")
    return jsonify(output_niches)

@app.route('/')
def index():
    # Basic health check / info endpoint
    if niche_data_df is None or niche_data_df.empty:
        return "Niche Hunter Backend is running, but data is NOT loaded. Check CSV and logs."
    return f"Niche Hunter Backend is running. {len(niche_data_df)} keywords loaded. Ready to serve niches from your data!"

if __name__ == '__main__':
    # This is for local development. Render uses Gunicorn specified in Procfile or Start Command.
    # The PORT environment variable is typically set by Render.
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True) # debug=True is helpful for local dev
