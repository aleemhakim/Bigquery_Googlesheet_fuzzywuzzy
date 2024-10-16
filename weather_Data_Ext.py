from google.cloud import bigquery
import pandas as pd
import gspread
from fuzzywuzzy import process
import pandas as pd



##Extracting the big query weather mete data 
# Path to your service account JSON key file
service_account_file = 'bq_login.json'

# Create a BigQuery client using the service account
client = bigquery.Client.from_service_account_json(service_account_file)

# Define the query to get all data from the table
query = """
SELECT *
FROM `Table_name_big_query`
"""

# Execute the query
query_job = client.query(query)

# Convert the results to a pandas DataFrame
weather_meta_data_df = query_job.to_dataframe()

# Display the DataFrame
# print(weather_meta_data_df)


##extracting the sheet data

# Connect to Google Sheets
gc = gspread.service_account(filename='service_account_sheets.json')

# Open the Google Sheet by URL or ID
spreadsheet_url = "add_your_sheet_url"
spreadsheet = gc.open_by_url(spreadsheet_url)

# Select the first worksheet
worksheet = spreadsheet.sheet1

# Get all values from the worksheet
data = worksheet.get_all_records()

# Convert the data into a pandas DataFrame
sites_df = pd.DataFrame(data)

# Display the DataFrame
print(sites_df)

# Get unique circle names from the 'Circle' column without normalization
unique_circles = sites_df['Circle'].unique()


#Extracting the loc_id as well with name of circle and city comparisons

# Normalize the city names in weather_meta_data_df (lowercase and strip)
weather_meta_data_df['city_cleaned'] = weather_meta_data_df['city'].str.lower().str.strip()

# Function to get best match using fuzzy matching
def fuzzy_match_city(city, choices, threshold=80):
    # Get the best match using process.extractOne()
    best_match = process.extractOne(city, choices, score_cutoff=threshold)
    return best_match

# Prepare list of cities from weather_meta_data_df for matching
weather_cities = weather_meta_data_df['city_cleaned'].unique()

# Create a dictionary to store the matches
fuzzy_matches = {}

# Compare each city in unique_circles with the cities in weather_meta_data_df
for city in unique_circles:
    best_match = fuzzy_match_city(city.lower(), weather_cities)
    if best_match:
        # Find the loc_id for the matched city
        matched_city_row = weather_meta_data_df[weather_meta_data_df['city_cleaned'] == best_match[0]]
        loc_id = matched_city_row['loc_id'].values[0]  # Get the loc_id for the matched city
        fuzzy_matches[city] = (best_match[0], loc_id)  # Store the matched city and loc_id

# Print the fuzzy matching results with loc_id
print("Fuzzy Matching Results:")
for circle_city, (matched_city, loc_id) in fuzzy_matches.items():
    print(f"Circle City: {circle_city} --> Matched City: {matched_city}, loc_id: {loc_id}")


##Creating new df for the matched sites
# Create a list to store the rows for the new DataFrame
rows = []

# Loop through the fuzzy matches and add rows with circle city, matched city, and loc_id
for circle_city, (matched_city, loc_id) in fuzzy_matches.items():
    rows.append({
        'Circle': circle_city,
        'City': matched_city,
        'loc_id': loc_id
    })

# Create a new DataFrame with the data
matched_cities_df = pd.DataFrame(rows)

# Display the resulting DataFrame
print(matched_cities_df)


##Extracting weather data with daily frequency and saving it to csv file

# Path to your service account JSON key file
service_account_file = 'bq_login.json'

# Create a BigQuery client using the service account
client = bigquery.Client.from_service_account_json(service_account_file)

# Define your list of site loc_ids (assumed to come from matched_cities_df)
loc_ids = matched_cities_df['loc_id'].tolist()

# List to store all the data from each site
all_site_data = []

# Function to query daily aggregated data for a given loc_id
def get_daily_weather_data(loc_id):
    query = f"""
        SELECT loc_id, 
               DATE(timestamp) as date, 
               MAX(temperature_2m) as max_temperature_2m, 
               SUM(rain) as total_rain
        FROM `web-server-for-ops.weather_database.weather_raw`
        WHERE loc_id = @loc_id
        AND timestamp >= '2020-01-01'
        GROUP BY loc_id, date
        ORDER BY date
    """

    # Set the query parameter
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("loc_id", "INT64", loc_id)
        ]
    )

    # Run the query and get the result as a pandas DataFrame
    query_job = client.query(query, job_config=job_config)
    return query_job.to_dataframe()

# Iterate through each loc_id and fetch its daily weather data
for loc_id in loc_ids:
    print(f"Fetching daily data for site loc_id: {loc_id}")
    
    # Get the data for this site
    site_data = get_daily_weather_data(loc_id)
    
    # Append the data to the list
    all_site_data.append(site_data)

# Concatenate all the site data into a single DataFrame
final_weather_data_df = pd.concat(all_site_data, ignore_index=True)

# Display the resulting DataFrame
print(final_weather_data_df)

# Optionally, you can save this DataFrame to a CSV file
final_weather_data_df.to_csv('daily_weather_data.csv', index=False)
print("Daily weather data has been saved to 'daily_weather_data1.csv'.")
