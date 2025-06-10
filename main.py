from flask import Flask, request, jsonify
import csv
import io
import requests
import json
import os
import time

BOX_ACCESS_TOKEN = os.environ.get("BOX_ACCESS_TOKEN")
EXPECTED_TOKEN = os.environ.get("EXPECTED_TOKEN")

app = Flask(__name__)

# ── Configurable settings ─────────────────────────────
BOX_FOLDER_ID = "325307519819"
MASTER_FILENAME = "data_master.csv"

# Box API endpoints
BOX_UPLOAD_URL = "https://upload.box.com/api/2.0/files/content"
BOX_SEARCH_URL = "https://api.box.com/2.0/search"
BOX_DOWNLOAD_URL = "https://api.box.com/2.0/files/{file_id}/content"
BOX_UPDATE_URL = "https://upload.box.com/api/2.0/files/{file_id}/content"

# Use a session for connection pooling and default headers
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {BOX_ACCESS_TOKEN}"})

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
    # If we still can't connect after retries, fail safe
    raise ConnectionError(f"Unable to check for existing file '{filename}' on Box after {retries} attempts.")

def upload_file(filename, content):
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
