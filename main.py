import os
import io
import csv
import json
import time
from flask import Flask, request, jsonify
import requests
from datetime import datetime, timedelta
from dateutil import parser
import jwt  # PyJWT, not the built-in Python 'jwt' package

# ------------------------ JWT CONFIGURATION ------------------------
BOX_CLIENT_ID      = os.environ.get("BOX_CLIENT_ID")
BOX_CLIENT_SECRET  = os.environ.get("BOX_CLIENT_SECRET")
BOX_APP_USER_ID    = os.environ.get("BOX_APP_USER_ID")   
BOX_JWT_PRIVATE_KEY = os.environ.get("BOX_JWT_PRIVATE_KEY") 
BOX_JWT_PASSPHRASE = os.environ.get("BOX_JWT_PASSPHRASE")
EXPECTED_TOKEN     = os.environ.get("EXPECTED_TOKEN")

DEFAULT_BOX_FOLDER_ID = "314409658870"

# Box API endpoints
BOX_TOKEN_URL = "https://api.box.com/oauth2/token"
BOX_UPLOAD_URL = "https://upload.box.com/api/2.0/files/content"
BOX_SEARCH_URL = "https://api.box.com/2.0/search"
BOX_DOWNLOAD_URL = "https://api.box.com/2.0/files/{file_id}/content"
BOX_UPDATE_URL = "https://upload.box.com/api/2.0/files/{file_id}/content"

# ------------------------ JWT TOKEN HANDLING -----------------------
access_token = None
token_expires_at = None

def get_jwt_assertion():
    claims = {
        'iss': BOX_CLIENT_ID,
        'sub': BOX_APP_USER_ID,        
        'box_sub_type': 'user',     
        'aud': BOX_TOKEN_URL,
        'jti': os.urandom(24).hex(),
        'exp': int(time.time()) + 45
    }
    key_pass = BOX_JWT_PASSPHRASE if BOX_JWT_PASSPHRASE else None
    return jwt.encode(
        claims,
        BOX_JWT_PRIVATE_KEY,
        algorithm='RS512',
        passphrase=key_pass
    )

def refresh_access_token():
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
    global access_token, token_expires_at
    if access_token and token_expires_at and datetime.utcnow() < token_expires_at:
        return access_token
    return refresh_access_token()

def get_session():
    token = get_access_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session

# ------------------------ FLASK SETUP -----------------------------
app = Flask(__name__)

def _to_csv(group_row, question_row, data_row):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(group_row)
    writer.writerow(question_row)
    writer.writerow(data_row)
    return buf.getvalue()

def get_unique_filename(base_filename, folder_id):
    name, ext = os.path.splitext(base_filename)
    count = 1
    filename = base_filename
    while file_exists_in_box(filename, folder_id):
        filename = f"{name}_{count}{ext}"
        count += 1
    return filename

def file_exists_in_box(filename, folder_id, retries=3, delay=2):
    folder_url = f"https://api.box.com/2.0/folders/{folder_id}/items"
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
        except Exception as e:
            print(f"⚠️ Box connection failed (attempt {attempt+1}): {e}")
            time.sleep(delay)
    raise ConnectionError(f"Unable to check file '{filename}' in Box after {retries} attempts.")

def upload_file(filename, content, folder_id):
    session = get_session()
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

def find_master_file(folder_id, master_filename):
    session = get_session()
    params = {
        "query": master_filename,
        "ancestor_folder_ids": folder_id,
        "type": "file"
    }
    resp = session.get(BOX_SEARCH_URL, params=params)
    if resp.status_code == 200:
        entries = resp.json().get("entries", [])
        for entry in entries:
            if entry.get("name") == master_filename:
                return entry.get("id")
    folder_url = f"https://api.box.com/2.0/folders/{folder_id}/items"
    resp = session.get(folder_url)
    if resp.status_code == 200:
        entries = resp.json().get("entries", [])
        for entry in entries:
            if entry.get("name") == master_filename and entry.get("type") == "file":
                return entry.get("id")
    return None

def update_master_csv(fieldnames, group_row, question_row, data_row, folder_id, master_filename):
    session = get_session()
    file_id = find_master_file(folder_id, master_filename)
    if file_id:
        resp = session.get(BOX_DOWNLOAD_URL.format(file_id=file_id))
        if resp.status_code == 200:
            existing_content = resp.content.decode()
            existing_reader = csv.reader(io.StringIO(existing_content))
            existing_rows = list(existing_reader)
            updated_group_row = group_row
            updated_question_row = question_row
            existing_data_rows = existing_rows[2:] if len(existing_rows) >= 2 else []
            all_data_rows = existing_data_rows + [data_row]
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(updated_group_row)
            writer.writerow(updated_question_row)
            writer.writerows(all_data_rows)
            files = {'file': (master_filename, buf.getvalue(), 'text/csv')}
            resp2 = session.post(BOX_UPDATE_URL.format(file_id=file_id), files=files)
            if resp2.status_code == 201:
                print(f"✅ Updated master CSV ({len(all_data_rows)} rows)")
            else:
                print(f"❌ Master update failed ({resp2.status_code}): {resp2.text}")
        else:
            print(f"❌ Failed to download master ({resp.status_code})")
    else:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(group_row)
        writer.writerow(question_row)
        writer.writerow(data_row)
        upload_file(master_filename, buf.getvalue(), folder_id)
        print("✅ Created new master CSV")

def get_formatted_date(response_data):
    raw_date = response_data.get("date")
    if not raw_date or not str(raw_date).strip():
        return datetime.now().strftime("%m-%d-%Y")
    date_str = str(raw_date).strip().replace("/", "-")
    try:
        dt = parser.parse(date_str, dayfirst=False, yearfirst=False)
        return dt.strftime("%m-%d-%Y")
    except Exception as e:
        print(f"⚠️ Could not parse date '{raw_date}', defaulting to today. ({e})")
        return datetime.now().strftime("%m-%d-%Y")

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    if data.get("token") != EXPECTED_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    folder_id = data.get("box_folder_id", DEFAULT_BOX_FOLDER_ID)
    source = data.get("source", "unknown")

    # Use the date from the response if available, else fallback
    response_data = data.get("response", {})
    formatted_date_str = get_formatted_date(response_data)
    master_filename = f"slb_{source}_master_{formatted_date_str}.csv"

    do_master = data.get("master") if "master" in data else True

    order = data.get("order", [])
    groupings = data.get("groupings", {})
    questions = data.get("questions", {})
    print(f"✅ Received Data for folder {folder_id}: {response_data}")

    fieldnames = order
    group_row = [groupings.get(f, f) for f in fieldnames]
    question_row = [questions.get(f, f) for f in fieldnames]
    data_row = [response_data.get(f, "") for f in fieldnames]

    participant_id = response_data.get("participantID", "unknown")
    individual_name = f"slb_feedback_{participant_id}_{formatted_date_str}.csv"
    try:
        unique_name = get_unique_filename(individual_name, folder_id)
        upload_file(unique_name, _to_csv(group_row, question_row, data_row), folder_id)
    except Exception as e:
        print(f"❌ Upload file error: {e}")

    if do_master:
        try:
            update_master_csv(fieldnames, group_row, question_row, data_row, folder_id, master_filename)
        except Exception as e:
            print(f"❌ Master update error: {e}")

    return jsonify({"status": "success"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
