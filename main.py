from __future__ import print_function
import os
import csv
import io
import requests
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- Configuration ---
# Only Sheets scope is needed now
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SPREADSHEET_ID = os.getenv('GOOGLE_SHEET_ID')
SHEET_NAMES = os.getenv('SHEETS')  # Semicolon separated
ENDPOINTS = os.getenv('ENDPOINT')  # Semicolon separated

SERVICE_ACCOUNT_FILE = "credentials.json"  # Path to your service account key file

def authenticate():
    """Authenticates and returns only the Sheets service."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)

def get_max_id(rows, id_col_index=0):
    """Finds the maximum integer ID in a list of rows."""
    max_id = 0
    for row in rows:
        try:
            if len(row) > id_col_index:
                val = int(row[id_col_index])
                if val > max_id:
                    max_id = val
        except ValueError:
            continue 
    return max_id

def process_and_update(service, endpoints, sheet_names):
    url_list = endpoints.split(';')
    sheet_list = sheet_names.split(';')
    loop_count = min(len(url_list), len(sheet_list))

    for i in range(loop_count):
        url = url_list[i]
        target_sheet_name = sheet_list[i]
        
        print(f"\n--- Processing {target_sheet_name} ---")

        # 1. Download Content
        try:

            print(f"Downloading from: {url}")
            response = requests.get(url)
            response.raise_for_status()
    
            csv_content = response.content.decode("utf-8")
        except Exception as e:
                print(f"❌ Error downloading {url}: {e}")
                continue

        # 2. Parse CSV
        csv_reader = csv.reader(io.StringIO(csv_content))
        csv_rows = list(csv_reader)
        
        if not csv_rows:
            print("⚠️ CSV is empty, skipping update.")
            continue

        header = csv_rows[0]
        new_data_rows = csv_rows[1:]
        column_count = len(header)

        # 3. Fetch Existing Sheet Data
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=target_sheet_name
        ).execute()
        existing_values = result.get('values', [])

        # 4. Filter New Rows
        rows_to_append = []
        
        if not existing_values:
            # If sheet is empty, add header + all data
            print("Sheet is empty. Appending all data.")
            rows_to_append = csv_rows # Includes header
        else:
            # Identify ID column (default to 0 if 'id' not found)
            try:
                id_col_idx = [h.lower() for h in header].index('id')
            except ValueError:
                id_col_idx = 0
            
            # Find max ID in current sheet
            existing_data_only = existing_values[1:] if len(existing_values) > 0 else []
            current_max_id = get_max_id(existing_data_only, id_col_idx)
            print(f"Current Max ID in Sheet: {current_max_id}")

            # Filter for newer IDs
            for row in new_data_rows:
                try:
                    # Handle cases where row might be shorter than header
                    if len(row) > id_col_idx:
                        row_id = int(row[id_col_idx])
                        if row_id > current_max_id:
                            rows_to_append.append(row)
                except (ValueError, IndexError):
                    continue

        # 5. Append Data + Timestamp Row
        if rows_to_append:
            print(f"Appending {len(rows_to_append)} new rows...")
            
            # A. Prepare the Timestamp Row
            # Logic: ["Text", "", "", ...] to match column count
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message = f"journal updated at {timestamp}"
            
            # Create a list: [message, "", "", "" ...]
            separator_row = [message] + [""] * (column_count - 1)
            
            # Add separator to the end of the data chunk
            rows_to_append.append(separator_row)

            # B. Send to Google Sheets
            service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=target_sheet_name,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": rows_to_append}
            ).execute()
            
            print("✅ Data and timestamp appended successfully.")
        else:
            print("No new data found to append.")

def main():
    service = authenticate()
    process_and_update(service, ENDPOINTS, SHEET_NAMES)

if __name__ == '__main__':
    main()