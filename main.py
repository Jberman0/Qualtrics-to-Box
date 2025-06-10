from flask import Flask, request, jsonify
import csv
import io
import requests
import json
import os

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

    response_data = data.get("response", {})
    print("✅ Received Qualtrics Data:", response_data)

    # Prepare CSV content
    fieldnames = list(response_data.keys())
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow(response_data)
    csv_content = buffer.getvalue()

    # Save individual response
    participant_id = response_data.get("Participant_ID", "unknown")
    individual_name = f"slb_{participant_id}.csv"
    upload_file(individual_name, csv_content)

    # Update or create master CSV
    update_master_csv(fieldnames, response_data)

    return jsonify({"status": "success"}), 200


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


def update_master_csv(fieldnames, new_row):
    file_id = find_master_file()

    if file_id:
        # Download existing master
        resp = session.get(BOX_DOWNLOAD_URL.format(file_id=file_id))
        if resp.status_code == 200:
            existing_content = resp.content.decode()

            # Parse existing CSV
            existing_reader = csv.DictReader(io.StringIO(existing_content))
            existing_rows = list(existing_reader)
            existing_fieldnames = existing_reader.fieldnames or []

            # Merge fieldnames
            all_fieldnames = list(existing_fieldnames)
            for field in fieldnames:
                if field not in all_fieldnames:
                    all_fieldnames.append(field)

            # Create updated CSV
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=all_fieldnames)
            writer.writeheader()

            for row in existing_rows:
                writer.writerow(row)
            writer.writerow(new_row)

            # Update file
            files = {'file': (MASTER_FILENAME, buf.getvalue(), 'text/csv')}
            resp2 = session.post(BOX_UPDATE_URL.format(file_id=file_id), files=files)

            if resp2.status_code == 201:
                print(f"✅ Updated master CSV (now {len(existing_rows) + 1} rows)")
            else:
                print(f"❌ Master update failed ({resp2.status_code})")
        else:
            print(f"❌ Failed to download master ({resp.status_code})")
    else:
        # Create new master file
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(new_row)
        upload_file(MASTER_FILENAME, buf.getvalue())
        print("✅ Created new master CSV")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
