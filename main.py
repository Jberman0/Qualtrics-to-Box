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

# ------------------------ CONFIGURATION ------------------------
BOX_CLIENT_ID = os.environ.get("BOX_CLIENT_ID")
BOX_CLIENT_SECRET = os.environ.get("BOX_CLIENT_SECRET")
BOX_ENTERPRISE_ID = os.environ.get("BOX_ENTERPRISE_ID")
BOX_JWT_PRIVATE_KEY = os.environ.get("BOX_JWT_PRIVATE_KEY")
EXPECTED_TOKEN = os.environ.get("EXPECTED_TOKEN")
DEFAULT_BOX_FOLDER_ID = "314409658870"

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

def get_unique_filename(base_filename, entries):
    """Generate unique filename by appending counter if needed."""
    name, ext = os.path.splitext(base_filename)
    count = 1
    filename = base_filename
    
    while get_file_id_from_entries(filename, entries):
        filename = f"{name}_{count}{ext}"
        count += 1
    return filename

def upload_file(session, filename, content, folder_id):
    """Upload file to Box folder."""
    files = {
        'attributes': (None, json.dumps({"name": filename, "parent": {"id": folder_id}}), 'application/json'),
        'file': (filename, content, 'text/csv')
    }
    
    resp = session.post(BOX_UPLOAD_URL, files=files)
    if resp.status_code == 201:
        print(f"✅ Uploaded {filename}")
    elif resp.status_code == 409:
        print(f"⚠️ File {filename} already exists")
    else:
        print(f"❌ Upload failed ({resp.status_code}): {resp.text}")

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

def update_master_csv(session, fieldnames, group_row, question_row, data_row, 
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

# ------------------------ UTILITY FUNCTIONS ------------------------
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



def process_individual_file_upload(session, data, entries, folder_id, 
                                 group_row, question_row, data_row, 
                                 source, study_type, formatted_date_str):
    """Handle individual participant file upload."""
    response_data = data.get("response", {})
    participant_id = response_data.get("participantID", "unknown")
    individual_name = f"{study_type}_{source}_{participant_id}_{formatted_date_str}.csv"
    
    try:
        unique_name = get_unique_filename(individual_name, entries)
        csv_content = create_csv_content(group_row, question_row, data_row)
        upload_file(session, unique_name, csv_content, folder_id)
        return True
    except Exception as e:
        print(f"❌ Individual file upload error: {e}")
        return False

def process_master_file_update(session, data, entries, folder_id,
                              fieldnames, group_row, question_row, data_row,
                              source, study_type, formatted_date_str):
    """Handle master CSV file update."""
    do_master = data.get("master", True)  # Default to True if not specified
    
    if not do_master:
        print("ℹ️ Skipping master file update (master=false)")
        return True
    
    try:
        update_master_csv(session, fieldnames, group_row, question_row, data_row,
                         folder_id, source, study_type, formatted_date_str, entries)
        return True
    except Exception as e:
        print(f"❌ Master update error: {e}")
        return False

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
    source = data.get("source", "unknown")
    study_type = data.get("study_type", "fMRI")
    response_data = data.get("response", {})
    formatted_date_str = get_formatted_date(response_data)
    
    order = data.get("order", [])
    groupings = data.get("groupings", {})
    questions = data.get("questions", {})
    
    print(f"✅ Received data for source '{source}', study '{study_type}', date '{formatted_date_str}'")
    
    # Prepare CSV rows
    fieldnames = order
    group_row = [groupings.get(f, f) for f in fieldnames]
    question_row = [questions.get(f, f) for f in fieldnames]
    data_row = [response_data.get(f, "") for f in fieldnames]
    
    # Setup Box session and folder
    try:
        session = get_session()
        requested_folder_id = data.get("box_folder_id")
        folder_id = ensure_valid_folder_id(session, requested_folder_id)
        entries = get_folder_entries(session, folder_id)
        
        if entries is None:
            # Fallback to default folder
            folder_id = DEFAULT_BOX_FOLDER_ID
            entries = get_folder_entries(session, folder_id)
            
    except Exception as e:
        return jsonify({"status": "error", "message": f"Box authentication failed: {str(e)}"}), 500
    
    # Process uploads
    success_count = 0
    
    # Individual file upload
    if process_individual_file_upload(session, data, entries, folder_id,
                                    group_row, question_row, data_row,
                                    source, study_type, formatted_date_str):
        success_count += 1
    
    # Master file update
    if process_master_file_update(session, data, entries, folder_id,
                                fieldnames, group_row, question_row, data_row,
                                source, study_type, formatted_date_str):
        success_count += 1
    
    if success_count > 0:
        return jsonify({"status": "success", "message": f"Processed {success_count} operations"}), 200
    else:
        return jsonify({"status": "error", "message": "All operations failed"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)
