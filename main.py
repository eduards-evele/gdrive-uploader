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
LOCAL_BACKUP_DIR = os.getenv('LOCAL_BACKUP_DIR', '')  # Semicolon separated directories (must match endpoint count)
SERVICE_ACCOUNT_FILE = "credentials.json"

def authenticate():
    """Authenticates and returns the Sheets service."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)

def save_locally(content, backup_dir):
    """Saves CSV content to specified directory with timestamp."""
    if not backup_dir:
        return

    if not os.path.exists(backup_dir):
        try:
            os.makedirs(backup_dir)
        except OSError as e:
            print(f"❌ Could not create backup directory: {e}")
            return

    timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    filename = f"data_backup_{timestamp}.csv"
    filepath = os.path.join(backup_dir, filename)

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

def normalize_row(row, column_count):
    """Normalize row to have consistent length by padding with empty strings."""
    return row + [''] * (column_count - len(row))

def rows_are_different(row1, row2, column_count):
    """Compare two rows after normalizing their length."""
    norm_row1 = normalize_row(row1, column_count)
    norm_row2 = normalize_row(row2, column_count)
    return norm_row1 != norm_row2

def merge_rows(existing_row, new_row, column_count):
    """
    Merge two rows, preserving existing data when new row has empty columns.
    Returns a normalized row where:
    - Non-empty values from new_row are used
    - Empty values in new_row preserve the existing_row values
    """
    norm_existing = normalize_row(existing_row, column_count)
    norm_new = normalize_row(new_row, column_count)

    merged = []
    for i in range(column_count):
        # Use new value if it's non-empty, otherwise keep existing
        if norm_new[i] and norm_new[i].strip():
            merged.append(norm_new[i])
        else:
            merged.append(norm_existing[i])

    return merged

def process_and_update(service, endpoints, sheet_names):
    url_list = endpoints.split(';')
    sheet_list = sheet_names.split(';')
    backup_dir_list = LOCAL_BACKUP_DIR.split(';') if LOCAL_BACKUP_DIR else []

    # Validate that all lists have matching lengths
    if len(url_list) != len(sheet_list):
        raise ValueError(
            f"Endpoint count ({len(url_list)}) does not match sheet count ({len(sheet_list)})"
        )

    if backup_dir_list and len(backup_dir_list) != len(url_list):
        raise ValueError(
            f"Backup directory count ({len(backup_dir_list)}) does not match endpoint count ({len(url_list)})"
        )

    for i in range(len(url_list)):
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
        backup_dir = backup_dir_list[i] if backup_dir_list else None
        save_locally(csv_content, backup_dir)

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

        # 5. Process Rows - Detect New and Changed Rows
        rows_to_append = []
        rows_to_update = []

        if not existing_values:
            print("Sheet is empty. Appending all data.")
            rows_to_append = csv_rows # Includes header
        else:
            # Detect ID column index
            try:
                id_col_idx = [h.lower() for h in header].index('id')
            except ValueError:
                id_col_idx = 0

            # Build a dictionary of existing rows: {id: (row_index, row_data)}
            existing_data_only = existing_values[1:] if len(existing_values) > 0 else []
            existing_rows_map = {}

            for idx, row in enumerate(existing_data_only):
                try:
                    if len(row) > id_col_idx:
                        row_id = int(row[id_col_idx])
                        # Store 0-based data row index (header is row 0, first data row is row 1)
                        existing_rows_map[row_id] = (idx + 2, row)  # +2 because: +1 for header, +1 for 1-based indexing
                except (ValueError, IndexError):
                    continue

            print(f"Found {len(existing_rows_map)} existing rows with valid IDs")

            # Process each CSV row
            for row in new_data_rows:
                try:
                    if len(row) > id_col_idx:
                        row_id = int(row[id_col_idx])

                        if row_id in existing_rows_map:
                            # ID exists - check if data has changed
                            sheet_row_index, existing_row = existing_rows_map[row_id]

                            if rows_are_different(existing_row, row, column_count):
                                # Store both the row index and the existing row for merging
                                rows_to_update.append((sheet_row_index, row, existing_row))
                        else:
                            # New ID - append it
                            rows_to_append.append(row)
                except (ValueError, IndexError):
                    continue

        # 6. Update Changed Rows
        if rows_to_update:
            print(f"Updating {len(rows_to_update)} changed rows...")

            update_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            batch_data = []
            for row_index, new_row_data, existing_row_data in rows_to_update:
                # Merge rows: use new values where non-empty, preserve existing where CSV is empty
                merged_row = merge_rows(existing_row_data, new_row_data, column_count)

                # Append "updated at" timestamp to the last column (exists only in Google Sheets)
                merged_row.append(update_timestamp)

                # Create range like "Sheet1!A2:Z2" for the specific row
                range_name = f"{target_sheet_name}!A{row_index}"
                batch_data.append({
                    'range': range_name,
                    'values': [merged_row]
                })

            try:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body={'data': batch_data, 'valueInputOption': 'RAW'}
                ).execute()
                print(f"✅ Updated {len(rows_to_update)} rows successfully.")
            except Exception as e:
                print(f"❌ Error updating rows: {e}")

        # 7. Append New Rows + Timestamp Separator
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
            if not rows_to_update:
                print("No new or changed data found.")

def main():
    service = authenticate()
    process_and_update(service, ENDPOINTS, SHEET_NAMES)

if __name__ == '__main__':
    main()