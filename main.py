from flask import Flask, request, jsonify
import csv
import io
import requests
import json
import os
import time
from datetime import datetime, timedelta

# OAuth Configuration
BOX_CLIENT_ID = os.environ.get("BOX_CLIENT_ID")
BOX_CLIENT_SECRET = os.environ.get("BOX_CLIENT_SECRET")
BOX_REFRESH_TOKEN = os.environ.get("BOX_REFRESH_TOKEN")  # You'll get this from initial OAuth flow

EXPECTED_TOKEN = os.environ.get("EXPECTED_TOKEN")

app = Flask(__name__)

# ── Configurable settings ─────────────────────────────
BOX_FOLDER_ID = "325307519819"
MASTER_FILENAME = "data_master.csv"

# Box API endpoints
BOX_TOKEN_URL = "https://api.box.com/oauth2/token"
BOX_UPLOAD_URL = "https://upload.box.com/api/2.0/files/content"
BOX_SEARCH_URL = "https://api.box.com/2.0/search"
BOX_DOWNLOAD_URL = "https://api.box.com/2.0/files/{file_id}/content"
BOX_UPDATE_URL = "https://upload.box.com/api/2.0/files/{file_id}/content"

# Global variables for token management
access_token = None
refresh_token = BOX_REFRESH_TOKEN
token_expires_at = None

def refresh_access_token():
    """Refresh the access token using the refresh token"""
    global access_token, refresh_token, token_expires_at
    
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': BOX_CLIENT_ID,
        'client_secret': BOX_CLIENT_SECRET
    }
    
    response = requests.post(BOX_TOKEN_URL, data=data)
    
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data['access_token']
        refresh_token = token_data['refresh_token']  # Box gives you a new refresh token each time
        expires_in = token_data.get('expires_in', 3600)
        token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 60)  # Refresh 1 minute early
        print("✅ Successfully refreshed access token")
        return access_token
    else:
        print(f"❌ Failed to refresh token: {response.status_code} - {response.text}")
        raise Exception(f"Failed to refresh Box token: {response.text}")

def get_access_token():
    """Get a valid access token, refreshing if necessary"""
    global access_token, token_expires_at
    
    # Check if we have a valid token
    if access_token and token_expires_at and datetime.utcnow() < token_expires_at:
        return access_token
    
    # Token is expired or doesn't exist, refresh it
    return refresh_access_token()

def get_session():
    """Get a requests session with current access token"""
    token = get_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    if data.get("token") != EXPECTED_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    order = data.get("order", [])
    groupings = data.get("groupings", {})
    questions = data.get("questions", {})
    response_data = data.get("response", {})
    print("✅ Received Qualtrics Data:", response_data)
    print("Groupings:", groupings)
    print("Order:", order)

    fieldnames = order
    group_row = [groupings.get(f, f) for f in fieldnames]
    question_row = [questions.get(f, f) for f in fieldnames]
    data_row = [response_data.get(f, "") for f in fieldnames]

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(group_row)
    writer.writerow(question_row)
    writer.writerow(data_row)
    csv_content = buffer.getvalue()

    # Save individual response with unique filename
    participant_id = response_data.get("participantID", "unknown")
    individual_name = f"slb_{participant_id}.csv"
    try:
        unique_name = get_unique_filename(individual_name)
        upload_file(unique_name, csv_content)
    except ConnectionError as e:
        print(f"❌ {e}")
        return jsonify({"status": "error", "message": str(e)}), 503

    # Update or create master CSV
    update_master_csv(fieldnames, response_data, group_row, question_row, data_row)

    return jsonify({"status": "success"}), 200

def get_unique_filename(base_filename):
    name, ext = os.path.splitext(base_filename)
    count = 1
    filename = base_filename

    while file_exists_in_box(filename):
        filename = f"{name}_{count}{ext}"
        count += 1

    return filename

def file_exists_in_box(filename, retries=3, delay=2):
    folder_url = f"https://api.box.com/2.0/folders/{BOX_FOLDER_ID}/items"
    
    for attempt in range(retries):
        try:
            session = get_session()
            resp = session.get(folder_url, timeout=10)
            if resp.status_code == 200:
                entries = resp.json().get("entries", [])
                for entry in entries:
                    if entry.get("name") == filename and entry.get("type") == "file":
                        return True
                return False
            else:
                print(f"⚠️ Box folder listing failed ({resp.status_code}): {resp.text}")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Box connection failed (attempt {attempt+1}): {e}")
            time.sleep(delay)
        except Exception as e:
            print(f"⚠️ Authentication error (attempt {attempt+1}): {e}")
            # Reset token on auth error
            global access_token, token_expires_at
            access_token = None
            token_expires_at = None
            time.sleep(delay)
    
    raise ConnectionError(f"Unable to check for existing file '{filename}' on Box after {retries} attempts.")

def upload_file(filename, content):
    session = get_session()
    files = {
        'attributes': (None, json.dumps({"name": filename, "parent": {"id": BOX_FOLDER_ID}}), 'application/json'),
        'file': (filename, content, 'text/csv')
    }
    resp = session.post(BOX_UPLOAD_URL, files=files)

    if resp.status_code == 201:
        print(f"✅ Uploaded {filename}")
    elif resp.status_code == 409:
        print(f"⚠️ File {filename} already exists")
    else:
        print(f"❌ Upload failed ({resp.status_code}): {resp.text}")

def find_master_file():
    session = get_session()
    
    # Try search first
    params = {
        "query": MASTER_FILENAME,
        "ancestor_folder_ids": BOX_FOLDER_ID,
        "type": "file"
    }

    resp = session.get(BOX_SEARCH_URL, params=params)
    if resp.status_code == 200:
        entries = resp.json().get("entries", [])
        for entry in entries:
            if entry.get("name") == MASTER_FILENAME:
                return entry.get("id")

    # Fallback to folder listing
    folder_url = f"https://api.box.com/2.0/folders/{BOX_FOLDER_ID}/items"
    resp = session.get(folder_url)
    if resp.status_code == 200:
        entries = resp.json().get("entries", [])
        for entry in entries:
            if entry.get("name") == MASTER_FILENAME and entry.get("type") == "file":
                return entry.get("id")

    return None

def update_master_csv(fieldnames, response_data, group_row, question_row, data_row):
    session = get_session()
    file_id = find_master_file()

    if file_id:
        # Download existing master
        resp = session.get(BOX_DOWNLOAD_URL.format(file_id=file_id))
        if resp.status_code == 200:
            existing_content = resp.content.decode()
            existing_reader = csv.reader(io.StringIO(existing_content))
            existing_rows = list(existing_reader)

            # Always use current group, question, and field order
            updated_group_row = group_row
            updated_question_row = question_row

            # All rows after the headers are data rows
            existing_data_rows = existing_rows[2:] if len(existing_rows) >= 2 else []

            # Append new row to the previous data rows
            all_data_rows = existing_data_rows + [data_row]

            # Write back
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(updated_group_row)
            writer.writerow(updated_question_row)
            writer.writerows(all_data_rows)

            files = {'file': (MASTER_FILENAME, buf.getvalue(), 'text/csv')}
            resp2 = session.post(BOX_UPDATE_URL.format(file_id=file_id), files=files)

            if resp2.status_code == 201:
                print(f"✅ Updated master CSV (now {len(all_data_rows)} rows)")
            else:
                print(f"❌ Master update failed ({resp2.status_code})")
        else:
            print(f"❌ Failed to download master ({resp.status_code})")
    else:
        # Create new master file
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(group_row)
        writer.writerow(question_row)
        writer.writerow(data_row)
        upload_file(MASTER_FILENAME, buf.getvalue())
        print("✅ Created new master CSV")

# Helper route to get initial OAuth URL (run this once to get your refresh token)
@app.route("/oauth-url", methods=["GET"])
def oauth_url():
    """Generate OAuth URL for initial authorization - visit this URL once to get your refresh token"""
    # Get the base URL from the request (works for both local and Render)
    base_url = request.url_root.rstrip('/')
    redirect_uri = f"{base_url}/oauth-callback"
    auth_url = f"https://account.box.com/api/oauth2/authorize?client_id={BOX_CLIENT_ID}&response_type=code&redirect_uri={redirect_uri}"
    return jsonify({"oauth_url": auth_url, "redirect_uri": redirect_uri, "message": "Visit this URL to authorize the app"})

@app.route("/oauth-callback", methods=["GET"])
def oauth_callback():
    """Handle OAuth callback and exchange code for tokens"""
    code = request.args.get('code')
    if not code:
        return jsonify({"error": "No authorization code received"}), 400
    
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'client_id': BOX_CLIENT_ID,
        'client_secret': BOX_CLIENT_SECRET
    }
    
    response = requests.post(BOX_TOKEN_URL, data=data)
    
    if response.status_code == 200:
        token_data = response.json()
        refresh_token = token_data['refresh_token']
        return jsonify({
            "message": "Success! Add this to your environment variables:",
            "BOX_REFRESH_TOKEN": refresh_token
        })
    else:
        return jsonify({"error": f"Failed to get tokens: {response.text}"}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
