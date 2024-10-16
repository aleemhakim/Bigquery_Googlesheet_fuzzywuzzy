# Bigquery_Googlesheet_fuzzywuzzy
# Weather Data Extraction & Processing Project

## Overview

This project extracts weather data from Google BigQuery and Google Sheets, processes it using Python, and stores it in a CSV file. The project demonstrates how to:
- Connect to BigQuery using a service account
- Extract raw weather data with 15-minute intervals
- Aggregate weather data to a daily frequency
- Perform fuzzy matching between data from BigQuery and Google Sheets
- Save the results to a CSV file

## How to Use

### 1. Setup

To use this project, you'll need to set up Google Cloud and Google Sheets API credentials.

1. **BigQuery Setup**:
   - Create a service account key for BigQuery, and download the `bq_login.json` file.
   - Place the `bq_login.json` file in your project directory (but **do not upload it to GitHub**).

2. **Google Sheets Setup**:
   - Enable the Google Sheets API and create a service account key, and download the `service_account_sheets.json` file.
   - Place the `service_account_sheets.json` file in your project directory.

3. **Environment Setup**:
   - Install required libraries by running:
     ```bash
     pip install google-cloud-bigquery gspread pandas fuzzywuzzy
     ```

### 2. Running the Script

1. Modify the necessary sections of the `main.py` script, such as:
   - BigQuery table name.
   - Google Sheets URL.
   
2. Run the script:
   ```bash
   python main.py
