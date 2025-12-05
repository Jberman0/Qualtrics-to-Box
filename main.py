import os
import io
import csv
import json
import time
from flask import Flask, request, jsonify
import requests
from datetime import datetime, timedelta
from dateutil import parser
import jwt
import pytz
import re
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# ------------------------ CONFIGURATION ------------------------
BOX_CLIENT_ID = os.environ.get("BOX_CLIENT_ID")
BOX_CLIENT_SECRET = os.environ.get("BOX_CLIENT_SECRET")
BOX_ENTERPRISE_ID = os.environ.get("BOX_ENTERPRISE_ID")
BOX_JWT_PRIVATE_KEY = os.environ.get("BOX_JWT_PRIVATE_KEY")
EXPECTED_TOKEN = os.environ.get("EXPECTED_TOKEN")
GOOGLE_SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
DEFAULT_BOX_FOLDER_ID = "314409658870"
FIRST_SCREENER_ROOT_FOLDER_ID = "329060221630"

# Define the questionnaire order
QUESTIONNAIRE_ORDER = [
    "demographics", "srs2", "cati", "stai", "bhitop", "lsas-sr", "phq9",
    "ius12", "oci-r", "pss", "scs10", "ucla-loneliness", "pq16"
]

# Box API endpoints
BOX_TOKEN_URL = "https://api.box.com/oauth2/token"
BOX_UPLOAD_URL = "https://upload.box.com/api/2.0/files/content"
BOX_DOWNLOAD_URL = "https://api.box.com/2.0/files/{file_id}/content"
BOX_UPDATE_URL = "https://upload.box.com/api/2.0/files/{file_id}/content"

# Global token cache
access_token = None
token_expires_at = None

# ------------------------ JWT AUTHENTICATION ------------------------
def get_jwt_assertion():
    """Generate JWT assertion for Box authentication."""
    claims = {
        'iss': BOX_CLIENT_ID,
        'sub': BOX_ENTERPRISE_ID,
        'box_sub_type': 'enterprise',
        'aud': BOX_TOKEN_URL,
        'jti': os.urandom(24).hex(),
        'exp': int(time.time()) + 45
    }
    return jwt.encode(claims, BOX_JWT_PRIVATE_KEY, algorithm='RS512')

def refresh_access_token():
    """Refresh Box access token using JWT."""
    global access_token, token_expires_at
    
    assertion = get_jwt_assertion()
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': assertion,
        'client_id': BOX_CLIENT_ID,
        'client_secret': BOX_CLIENT_SECRET
    }
    
    resp = requests.post(BOX_TOKEN_URL, data=data)
    if resp.status_code == 200:
        resp_json = resp.json()
        access_token = resp_json['access_token']
        expires_in = resp_json.get('expires_in', 3600)
        token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 60)
        print("✅ Refreshed Box access token")
        return access_token
    else:
        print(f"❌ JWT token refresh failed: {resp.status_code} - {resp.text}")
        raise Exception(f"Box JWT token refresh failed: {resp.text}")

def get_access_token():
    """Get valid access token, refreshing if necessary."""
    global access_token, token_expires_at
    
    if access_token and token_expires_at and datetime.utcnow() < token_expires_at:
        return access_token
    return refresh_access_token()

def get_session():
    """Get authenticated requests session."""
    token = get_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session

# ------------------------ BOX API HELPERS ------------------------
def get_folder_entries(session, folder_id):
    """Get entries for a Box folder. Returns None if folder doesn't exist."""
    folder_url = f"https://api.box.com/2.0/folders/{folder_id}/items"
    resp = session.get(folder_url, timeout=10)
    
    if resp.status_code == 200:
        return resp.json().get("entries", [])
    else:
        print(f"⚠️ Box folder listing failed ({resp.status_code}): {resp.text}")
        return None

def ensure_valid_folder_id(session, folder_id, default_folder_id=DEFAULT_BOX_FOLDER_ID):
    """Return folder_id if it exists, otherwise return default."""
    if not folder_id:
        return default_folder_id
        
    entries = get_folder_entries(session, folder_id)
    if entries is not None:
        return folder_id
    else:
        print(f"⚠️ Folder {folder_id} not found, defaulting to {default_folder_id}")
        return default_folder_id

def get_file_id_from_entries(filename, entries):
    """Get file_id from Box folder entries, or None if not found."""
    for entry in entries:
        if entry.get("name") == filename and entry.get("type") == "file":
            return entry.get("id")
    return None

# Unused but saving for later reference
def get_unique_filename(base_filename, entries, participant_id=None, questionnaire=None, study_type=None, source=None, date_str=None):
    """Generate unique filename by appending counter after participant_id if needed. Supports questionnaire in filename."""
    if participant_id and study_type and source and date_str:
        # Pattern: {study_type}_{source}_{questionnaire}_{participant_id}_{counter?}_{date_str}.csv
        count = 1
        if questionnaire != "unknown":
            filename = f"{study_type}_{source}_{questionnaire}_{participant_id}_{date_str}.csv"
            base_pattern = f"{study_type}_{source}_{questionnaire}_{participant_id}"
        else:
            filename = f"{study_type}_{source}_{participant_id}_{date_str}.csv"
            base_pattern = f"{study_type}_{source}_{participant_id}"
        # Find all files that match the base pattern (with or without counter)
        def matches_pattern(entry_name):
            if questionnaire != "unknown":
                prefix = f"{study_type}_{source}_{questionnaire}_{participant_id}"
            else:
                prefix = f"{study_type}_{source}_{participant_id}"
            return entry_name.startswith(prefix)
        existing_names = [e["name"] for e in entries if e.get("type") == "file" and matches_pattern(e["name"])]
        while filename in existing_names:
            if questionnaire and questionnaire != "unknown":
                filename = f"{study_type}_{source}_{questionnaire}_{participant_id}_{count}_{date_str}.csv"
            else:
                filename = f"{study_type}_{source}_{participant_id}_{count}_{date_str}.csv"
            count += 1
        return filename

def get_or_create_subfolder(session, folder_id, subfolder_name):
    """Check if a subfolder exists within the specified folder, create it if not, and return its ID.
    
    Args:
        session: Authenticated requests session for Box API calls.
        folder_id: ID of the Box folder to check for or create the subfolder in.
        subfolder_name: Name of the subfolder to check for or create (default: 'single_file').
    
    Returns:
        str: ID of the existing or newly created subfolder.
    """
    entries = get_folder_entries(session, folder_id)
    if entries is not None:
        for entry in entries:
            if entry.get("type") == "folder" and entry.get("name") == subfolder_name:
                return entry.get("id")
    
    # If not found, create the subfolder
    create_folder_url = "https://api.box.com/2.0/folders"
    data = {
        "name": subfolder_name,
        "parent": {"id": folder_id}
    }
    resp = session.post(create_folder_url, json=data)
    if resp.status_code == 201:
        new_folder_id = resp.json().get("id")
        print(f"✅ Created '{subfolder_name}' subfolder with ID {new_folder_id}")
        return new_folder_id
    else:
        print(f"❌ Failed to create '{subfolder_name}' subfolder: {resp.status_code} - {resp.text}")
        raise Exception(f"Could not create '{subfolder_name}' subfolder")

def upload_file(session, filename, content, folder_id):
    """Upload file to Box folder."""
    files = {
        'attributes': (None, json.dumps({"name": filename, "parent": {"id": folder_id}}), 'application/json'),
        'file': (filename, content, 'text/csv')
    }
    
    resp = session.post(BOX_UPLOAD_URL, files=files)
    if resp.status_code == 201:
        print(f"✅ Uploaded {filename}")
        return True
    elif resp.status_code == 409:
        print(f"⚠️ File {filename} already exists")
        return False
    else:
        print(f"❌ Upload failed ({resp.status_code}): {resp.text}")
        return False

def rename_file(session, file_id, new_name):
    """Rename a Box file."""
    patch_url = f"https://api.box.com/2.0/files/{file_id}"
    patch_data = {"name": new_name}
    
    resp = session.put(patch_url, 
                      data=json.dumps(patch_data),
                      headers={"Content-Type": "application/json"})
    
    if resp.status_code == 200:
        print(f"✅ Renamed to {new_name}")
    else:
        print(f"⚠️ Rename failed: {resp.text}")

# ------------------------ MASTER CSV LOGIC ------------------------
def find_source_master_file(entries, source, study_type):
    """Find the current master file for the given source (ignoring date)."""
    prefix = f"{study_type}_{source}_master_"
    
    for entry in entries or []:
        if entry.get("type") == "file" and entry.get("name", "").startswith(prefix):
            return entry["id"], entry["name"]
    return None, None

def extract_date_from_filename(filename, study_type, source):
    """Extract date from master filename like 'fMRI_source_master_01-15-2025.csv'"""
    expected_prefix = f"{study_type}_{source}_master_"
    if not filename.startswith(expected_prefix):
        return None
    
    # Extract date part (remove prefix and .csv extension)
    date_part = filename[len(expected_prefix):]
    if date_part.endswith('.csv'):
        date_part = date_part[:-4]
    
    try:
        # Parse the date string (MM-dd-yyyy format)
        return datetime.strptime(date_part, "%m-%d-%Y")
    except ValueError:
        return None

def should_update_master_filename(old_name, new_date_str, study_type, source):
    """Determine if master filename should be updated based on date comparison."""
    if not old_name:
        return True  # No existing file, so create with new date
    
    old_date = extract_date_from_filename(old_name, study_type, source)
    if not old_date:
        return True  # Couldn't parse old date, update to be safe
    
    try:
        new_date = datetime.strptime(new_date_str, "%m-%d-%Y")
        return new_date > old_date
    except ValueError:
        return False  # Couldn't parse new date, don't update

def download_existing_csv_content(session, file_id):
    """Download existing CSV content and return as list of rows."""
    resp = session.get(BOX_DOWNLOAD_URL.format(file_id=file_id))
    if resp.status_code == 200:
        return list(csv.reader(io.StringIO(resp.content.decode())))
    else:
        print(f"⚠️ Couldn't download existing file, starting fresh")
        return []

def update_master_csv(session, group_row, question_row, data_row, 
                     folder_id, source, study_type, formatted_date_str, entries):
    """
    Update master CSV file:
    1. Find the current master file for this source
    2. Download it (if it exists), append new row, upload
    3. Only rename if the new date is greater than the current master date
    """

    file_id, old_name = find_source_master_file(entries, source, study_type)
    new_master_name = f"{study_type}_{source}_master_{formatted_date_str}.csv"

    # Prepare CSV content
    buf = io.StringIO()
    writer = csv.writer(buf)

    if file_id:
        # Download and append to existing file
        existing_rows = download_existing_csv_content(session, file_id)
        if existing_rows:
            writer.writerows(existing_rows)  # Keep all previous data
        else:
            # If download failed, add headers
            writer.writerow(group_row)
            writer.writerow(question_row)
    else:
        # New file, add headers
        writer.writerow(group_row)
        writer.writerow(question_row)

    # Add new data row
    writer.writerow(data_row)
    csv_content = buf.getvalue()

    if file_id:
        # Update existing file content
        files = {'file': (old_name, csv_content, 'text/csv')}
        resp = session.post(BOX_UPDATE_URL.format(file_id=file_id), files=files)
        
        if resp.status_code in (200, 201):
            print(f"✅ Updated master content")
            
            # Only rename if new date is greater than old date
            if (old_name != new_master_name and 
                should_update_master_filename(old_name, formatted_date_str, study_type, source)):
                rename_file(session, file_id, new_master_name)
                print(f"✅ Renamed master file (new date {formatted_date_str} > old date)")
            elif old_name != new_master_name:
                print(f"ℹ️ Keeping old filename - new date {formatted_date_str} is not greater than existing date")
        else:
            print(f"❌ Master update failed: {resp.text}")
    else:
        # Create new master file
        upload_file(session, new_master_name, csv_content, folder_id)
        print("✅ Created new master CSV")

def create_csv_content(group_row, question_row, data_row):
    """Create CSV content from rows."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(group_row)
    writer.writerow(question_row)
    writer.writerow(data_row)
    return buf.getvalue()

def get_formatted_date(response_data):
    """Parse and format date from response data."""
    raw_date = response_data.get("date")
    tz = pytz.timezone('US/Eastern')
    today = datetime.now(tz).strftime("%m-%d-%Y")
    
    if not raw_date or not str(raw_date).strip():
        print(f"⚠️ No date found, defaulting to today - {today}.")
        return today
        
    date_str = str(raw_date).strip().replace("/", "-")
    try:
        dt = parser.parse(date_str, dayfirst=False, yearfirst=False)
        return dt.strftime("%m-%d-%Y")
    except Exception as e:
        print(f"⚠️ Could not parse date '{raw_date}', defaulting to today - {today}. ({e})")
        return today



def process_individual_file_upload(session, data, entries, participant_id, questionnaire, folder_id, 
                                 group_row, question_row, data_row, 
                                 source, study_type, formatted_date_str):

    """Handle individual participant file upload."""
    # Build intended filename
    if questionnaire != "unknown":
        individual_name = f"{study_type}_{source}_{questionnaire}_{participant_id}_{formatted_date_str}.csv"
    else:
        individual_name = f"{study_type}_{source}_{participant_id}_{formatted_date_str}.csv"

    try:
        csv_content = create_csv_content(group_row, question_row, data_row)
        upload_file(session, individual_name, csv_content, folder_id)
        return True
    except Exception as e:
        print(f"❌ Individual file upload error: {e}")
        return False

def process_master_file_update(session, data, entries, questionnaire, folder_id,
                              group_row, question_row, data_row,
                              source, study_type, formatted_date_str):
    """Handle master CSV file update."""
    do_master = data.get("master", True)  # Default to True if not specified
    
    if not do_master:
        print("ℹ️ Skipping master file update (master=false)")
        return True
    
    try:
        update_master_csv(session, group_row, question_row, data_row,
                         folder_id, source, study_type, formatted_date_str, entries)
        return True
    except Exception as e:
        print(f"❌ Master update error: {e}")
        return False

def apply_reversal_if_needed(response_data, reversal_config):
    """
    Return a copy of response_data with items ending in 'R' reversed if reversal_config is present.
    All other data is preserved unchanged.
    """
    if not reversal_config or not isinstance(reversal_config, dict):
        return response_data.copy()
    min_val = reversal_config.get("min")
    max_val = reversal_config.get("max")
    if min_val is None or max_val is None:
        return response_data.copy()
    updated_data = response_data.copy()
    for item, val in response_data.items():
        if isinstance(item, str) and item.endswith("R"):
            try:
                num = float(val)
                reversed_val = max_val - num + min_val
                if isinstance(val, int) or (isinstance(val, str) and val.isdigit()):
                    reversed_val = int(reversed_val)
                updated_data[item] = reversed_val
            except (TypeError, ValueError):
                pass
    return updated_data

def clean_pq16_data(response_data):
    """
    Clean PQ-16 data by clearing distressChoice and distressValue 
    when symptomChoice is False (or symptomValue is 0).
    
    Args:
        response_data (dict): The response data containing PQ-16 responses
        
    Returns:
        dict: Cleaned response data with distress values cleared when appropriate
    """
    cleaned_data = response_data.copy()

    for i in range(1, 17):
        symptom_value_key = f"QID53_{i}_symptomValue"
        distress_choice_key = f"QID53_{i}_distressChoice"
        distress_value_key = f"QID53_{i}_distressValue"

        # If symptomValue is 0, clear all distress entried
        symptom_value = cleaned_data.get(symptom_value_key, "")

        if str(symptom_value) == "0" :
            cleaned_data[distress_choice_key] = ""
            cleaned_data[distress_value_key] = ""

            print(f"Cleared values for item {i}")

    return cleaned_data

def merge_csvs_for_participant(session, folder_id, study_type, source, participant_id, formatted_date_str, entries, 
                        group_row, question_row, data_row, subfolder_name, QUESTIONNAIRE_ORDER, questionnaire=None, root_folder_id=None):
    """
    Horizontally merge all questionnaire CSVs for a participant/session (same date) into one CSV.
    - Each file has two header rows and one data row.
    - Keep only one set of participantID/date/time columns at the start.
    - Merge all other columns grouped by questionnaire, in the order specified by questionnaire_order.
    - Only one data row in the merged file (side-by-side merge).
    - The merged file is always saved in the 'single_file' subfolder under the root folder.
    """
    if questionnaire != "pq16":
        return False
    expected_prefix = f"{study_type}_{source}_"
    expected_suffix = f"_{participant_id}_{formatted_date_str}.csv"
    q_to_header = {}
    q_to_label = {}
    q_to_data = {}
    shared_cols = ["participantID", "date", "time"]
    for entry in entries:
        if entry.get("type") != "file":
            continue
        filename = entry["name"]
        if filename.startswith(expected_prefix) and filename.endswith(expected_suffix):
            q_part = filename[len(expected_prefix):-len(expected_suffix)]
            # Only include if questionnaire is in questionnaire_order
            if QUESTIONNAIRE_ORDER and q_part not in QUESTIONNAIRE_ORDER:
                continue
            file_id = entry["id"]
            resp = session.get(BOX_DOWNLOAD_URL.format(file_id=file_id))
            if resp.status_code == 200:
                csv_reader = list(csv.reader(io.StringIO(resp.content.decode())))
                if len(csv_reader) < 3:
                    continue
                q_to_header[q_part] = csv_reader[0]
                q_to_label[q_part] = csv_reader[1]
                q_to_data[q_part] = csv_reader[2]
            else:
                print(f"Failed to download {filename}")
    # Always add pq16 from the current in-memory data if not already present
    if "pq16" not in q_to_header:
        # Use the current in-memory data (group_row, question_row, data_row)
        q_to_header["pq16"] = group_row
        q_to_label["pq16"] = question_row
        q_to_data["pq16"] = data_row
    if not q_to_header:
        print("No questionnaire files found to merge.")
        return False
    # Build merged columns in the order: shared_cols + [all columns for each questionnaire in questionnaire_order]
    merged_header = []
    merged_label = []
    merged_data = []
    # Add shared columns from the first questionnaire in order
    first_q = QUESTIONNAIRE_ORDER[0] if QUESTIONNAIRE_ORDER and QUESTIONNAIRE_ORDER[0] in q_to_header else next(iter(q_to_header))
    first_header = q_to_header[first_q]
    first_label = q_to_label[first_q]
    first_data = q_to_data[first_q]
    for col in shared_cols:
        if col in first_header:
            idx = first_header.index(col)
            merged_header.append(col)
            merged_label.append(first_label[idx])
            merged_data.append(first_data[idx])
    # Add all columns for each questionnaire in order
    for q in (QUESTIONNAIRE_ORDER or list(q_to_header.keys())):
        if q not in q_to_header:
            continue
        header = q_to_header[q]
        label = q_to_label[q]
        data = q_to_data[q]
        for j, col in enumerate(header):
            if col in shared_cols:
                continue  # skip duplicate shared columns
            merged_header.append(col)
            merged_label.append(label[j])
            merged_data.append(data[j])
    # Write merged CSV
    merged_filename = f"{study_type}_{source}_{participant_id}_{formatted_date_str}_merged.csv"
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(merged_header)
    writer.writerow(merged_label)
    writer.writerow(merged_data)
    
    # Always use the root folder for the single_file subfolder
    if root_folder_id is None:
        root_folder_id = folder_id
    single_file_folder_id = get_or_create_subfolder(session, root_folder_id, subfolder_name)
    master_entries = get_folder_entries(session, root_folder_id)
    if upload_file(session, merged_filename, buf.getvalue(), single_file_folder_id):
        update_master_csv(session, merged_header, merged_label, merged_data, 
                     root_folder_id, source, study_type, formatted_date_str, master_entries)
        
        print(f"Horizontally merged CSV uploaded as {merged_filename} to '{subfolder_name}' subfolder")
        return True
    else:
        return False

def update_stratified_doc_screener(response_data):
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("SLB Stratified Sampling")

    # Sheet 1 - Symptoms (GAD-7, AQ-10, SMSAD)
    sheet_1 = spreadsheet.sheet1   

    # Get dataframe 
    symptoms_values = sheet_1.get("I14:M20")
    symptoms_list = list(symptoms_values)
    df_1 = pd.DataFrame(symptoms_list[1:], columns=symptoms_list[0]).reset_index(drop=True) 

    # particiantID
    participant_id = response_data.get("participantID", "").strip() or "unknownID"

    # Get values 
    aq_10_score = int(response_data.get("QID82_aq10_scoreRaw"))
    smsad_score = int(response_data.get("QID84_smsad_scoreAverage"))
    gad_7_score = int(response_data.get("QID83_gad7_scoreRaw"))

    # Symptoms dict
    scores_info = {
        "aq_10_score": {
            "score": aq_10_score,
            "threshold": 5,
            "low": [(3, 7), df_1.iloc[0, 3]],
            "high": [(3, 8), df_1.iloc[1, 3]]
        },
        "smsad_score": {
            "score": smsad_score,
            "threshold": 2,
            "low": [(6, 7), df_1.iloc[2, 3]],
            "high": [(6, 8), df_1.iloc[3, 3]]
        },
        "gad_7_score": {
            "score": gad_7_score,
            "threshold": 9,
            "low": [(9, 7), df_1.iloc[4, 3]],
            "high": [(9, 8), df_1.iloc[5, 3]]
        },
    }

    # ============= Symptoms Update =============
    for key, info in scores_info.items():
        score = info["score"]
        category = "low" if score <= info["threshold"] else "high"

        (row, col), old_val = info[category]

        new_val = int(old_val) + 1
        print(f"Updating {key} ({category}): {old_val} -> {new_val}")

        sheet_1.update_cell(row, col, new_val)

# ------------------------ FLASK APPLICATION ------------------------
app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    """Main webhook endpoint for processing CSV data."""
    try:
        data = request.get_json(force=True)
    except Exception as e:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400
    # Check token
    if data.get("token") != EXPECTED_TOKEN:
        return jsonify({"status": "forbidden"}), 403
    # Extract data
    source = data.get("source", "").strip() or "unknownSource"
    study_type = data.get("study_type", "fMRI")
    response_data = data.get("response", {})
    participant_id = response_data.get("participantID", "").strip() or "unknownID"
    formatted_date_str = get_formatted_date(response_data)
    questionnaire = data.get("questionnaire", data.get("questionnaire", "unknown"))
    config = {
        "order": data.get("order", []),
        "questions": data.get("questions", {})
    }
    print(f"✅ Received data for source '{source}', study '{study_type}', date '{formatted_date_str}'")
    # Apply reversal if reverse array is present
    reversal_config = data.get("reverse")

    # Fix pq-16 data (Qualtrics bug)
    if questionnaire == "pq16":
        response_data = clean_pq16_data(response_data)

    all_response_data = apply_reversal_if_needed(response_data, reversal_config)
    # Prepare CSV rows
    fieldnames = config["order"]
    group_row = fieldnames.copy()
    question_row = [config["questions"].get(f, f) for f in fieldnames]
    data_row = [all_response_data.get(f, "") for f in fieldnames]

    # Check if there is any data other than date and time (and participantID)
    non_empty_fields = [f for f in fieldnames if f not in ("date", "time", "participantID") and str(all_response_data.get(f, "")).strip() != ""]
    if not non_empty_fields:
        print(f"⚠️ No data other than date/time/participantID - {questionnaire} - skipping CSV write.")
        return jsonify({"status": "skipped", "message": "No data to write except date/time/participantID."}), 200
    
    # --- Participant/date subfolder logic ---
    SUBFOLDER_SOURCES = {"postScan"}

    try:
        session = get_session()
        requested_folder_id = data.get("box_folder_id")
        folder_id = ensure_valid_folder_id(session, requested_folder_id)
        entries = get_folder_entries(session, folder_id)
        root_folder_id = None
        if entries is None:
            folder_id = DEFAULT_BOX_FOLDER_ID
            entries = get_folder_entries(session, folder_id)
        # If source requires participant/date subfolder, get or create it
        if source in SUBFOLDER_SOURCES:
            subfolder_name = f"{participant_id}_{formatted_date_str}"
            root_folder_id = folder_id
            folder_id = get_or_create_subfolder(session, folder_id, subfolder_name)
            entries = get_folder_entries(session, folder_id)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Box authentication failed: {str(e)}"}), 500
    
    # Process uploads
    success_count = 0

    # Always append to the shared SLB master file for first-screener sources
    # Individual file upload
    individual_result = process_individual_file_upload(session, data, entries, participant_id, questionnaire, folder_id,
                                    group_row, question_row, data_row, source, study_type, formatted_date_str)
    if individual_result:
        success_count += 1
    else:
        print(f"ℹ️ Individual file upload failed or was skipped - checking for 409 conflict in logs")

    # Use the global QUESTIONNAIRE_ORDER variable for merging
    subfolder_name = "single_file"
    if merge_csvs_for_participant(session, folder_id, study_type, source, participant_id, formatted_date_str, entries,
                        group_row, question_row, data_row, subfolder_name, QUESTIONNAIRE_ORDER, questionnaire, root_folder_id):
        success_count += 1

    # Master file update (source-specific master)
    master_result = process_master_file_update(session, data, entries, questionnaire, folder_id,
                                group_row, question_row, data_row, source, study_type, 
                                formatted_date_str)
    if master_result:
        success_count += 1
        # If this is a first-screener source, also append the same data to the shared SLB master
        if source in {"firstScreenerAutistic", "firstScreenerNeurotypical"}:
            try:
                slb_root = FIRST_SCREENER_ROOT_FOLDER_ID
                slb_entries = get_folder_entries(session, slb_root)
                update_master_csv(session, group_row, question_row, data_row,
                                  slb_root, "firstScreener", "slb_fMRI", formatted_date_str, slb_entries)
                print(f"✅ Appended to SLB firstScreener master in folder {slb_root}")
            except Exception as e:
                print(f"❌ Appending to SLB master failed: {e}")

    if success_count > 0:
        return jsonify({"status": "success", "message": f"Processed {success_count} operations"}), 200
    else:
        return jsonify({"status": "error", "message": "All operations failed"}), 500

@app.route("/webhook2", methods=["POST"])
def webhook2():
    """Main webhook endpoint for updating stratified sampling spreadsheet."""
    try:
        data = request.get_json(force=True)
    except Exception as e:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400
    # Check token
    if data.get("token") != EXPECTED_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    try:
        response_data = data.get("response", {})
        final_status = response_data.get("finalStatus", "").lower()
        if final_status != "complete":
            print(f"ℹ️ Final status is '{final_status}' - skipping spreadsheet update.")
            return jsonify({"status": "skipped", "message": f"Final status is '{final_status}'."}), 200
        else:
            update_stratified_doc_screener(response_data)
            return jsonify({"status": "success", "message": "Stratified sampling spreadsheet updated"}), 200
    except Exception as e:
        print(f"❌ Error updating stratified sampling spreadsheet: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
        
@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)