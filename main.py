from __future__ import print_function
import os
import csv
import io
import requests
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- Configuration ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SPREADSHEET_ID = os.getenv('GOOGLE_SHEET_ID')
SHEET_NAMES = os.getenv('SHEETS')       # Semicolon separated (e.g., "Sheet1")
ENDPOINTS = os.getenv('ENDPOINT')       # Semicolon separated URLs
LOCAL_BACKUP_DIR = os.getenv('LOCAL_BACKUP_DIR', '') 
SERVICE_ACCOUNT_FILE = "credentials.json"

def authenticate():
    """Authenticates and returns the Sheets service."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)

def save_locally(content, index):
    """Saves CSV content to local directory with timestamp."""
    if not os.path.exists(LOCAL_BACKUP_DIR):
        try:
            os.makedirs(LOCAL_BACKUP_DIR)
        except OSError as e:
            print(f"❌ Could not create backup directory: {e}")
            return

    timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    filename = f"data_backup_{index}_{timestamp}.csv"
    filepath = os.path.join(LOCAL_BACKUP_DIR, filename)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"💾 Saved locally: {filepath}")
    except Exception as e:
        print(f"❌ Error saving local file: {e}")

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
    
    # Safely handle mismatched list lengths
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

        # 2. Save to Local Disk (Restored Feature)
        save_locally(csv_content, i+1)

        # 3. Parse CSV for Sheets Update
        csv_reader = csv.reader(io.StringIO(csv_content))
        csv_rows = list(csv_reader)
        
        if not csv_rows:
            print("⚠️ CSV is empty, skipping update.")
            continue

        header = csv_rows[0]
        new_data_rows = csv_rows[1:]
        column_count = len(header)

        # 4. Fetch Existing Sheet Data
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=target_sheet_name
            ).execute()
            existing_values = result.get('values', [])
        except Exception as e:
            print(f"❌ Error reading Google Sheet: {e}")
            continue

        # 5. Filter New Rows (Differential Logic)
        rows_to_append = []
        
        if not existing_values:
            print("Sheet is empty. Appending all data.")
            rows_to_append = csv_rows # Includes header
        else:
            # Detect ID column index
            try:
                id_col_idx = [h.lower() for h in header].index('id')
            except ValueError:
                id_col_idx = 0
            
            # Get Max ID
            existing_data_only = existing_values[1:] if len(existing_values) > 0 else []
            current_max_id = get_max_id(existing_data_only, id_col_idx)
            print(f"Current Max ID in Sheet: {current_max_id}")

            # Filter
            for row in new_data_rows:
                try:
                    if len(row) > id_col_idx:
                        row_id = int(row[id_col_idx])
                        if row_id > current_max_id:
                            rows_to_append.append(row)
                except (ValueError, IndexError):
                    continue

        # 6. Append Data + Timestamp Separator
        if rows_to_append:
            print(f"Appending {len(rows_to_append)} new rows...")
            
            # Create timestamp row: ["journal updated at ...", "", "", ...]
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message = f"journal updated at {timestamp}"
            separator_row = [message] + [""] * (column_count - 1)
            
            rows_to_append.append(separator_row)

            try:
                service.spreadsheets().values().append(
                    spreadsheetId=SPREADSHEET_ID,
                    range=target_sheet_name,
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": rows_to_append}
                ).execute()
                print("✅ Data and timestamp appended successfully.")
            except Exception as e:
                print(f"❌ Error appending to Sheet: {e}")
        else:
            print("No new data found to append.")

def main():
    service = authenticate()
    process_and_update(service, ENDPOINTS, SHEET_NAMES)

if __name__ == '__main__':
    main()