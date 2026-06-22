"""
Manual trigger for webhook and webhook2 logic — no Flask/HTTP needed.
Place this file in the same directory as main.py and run:
    python manual_main.py
"""

import json
import os
import sys
from dotenv import load_dotenv

load_dotenv("Box.env")

PAYLOAD_FILE = os.path.join(os.path.dirname(__file__), "payload.json")
with open(PAYLOAD_FILE) as f:
    PAYLOAD = json.load(f)


# ── Token check (mirrors the webhook guard) ──────────────────────────────────
EXPECTED_TOKEN = os.environ.get("EXPECTED_TOKEN", PAYLOAD.get("token"))

def check_token(data):
    if data.get("token") != EXPECTED_TOKEN:
        print("ERROR: Token mismatch — set EXPECTED_TOKEN env var or ensure payload token matches.")
        sys.exit(1)


# ── Run /webhook logic ───────────────────────────────────────────────────────
def run_webhook(data):
    print("\n" + "="*60)
    print("Running /webhook  (Box CSV upload + master file)")
    print("="*60)

    from main import (
        get_session, ensure_valid_folder_id, get_folder_entries,
        get_formatted_date, apply_reversal_if_needed, clean_pq16_data,
        process_individual_file_upload, merge_csvs_for_participant,
        process_master_file_update, DEFAULT_BOX_FOLDER_ID, QUESTIONNAIRE_ORDER
    )

    source        = data.get("source", "").strip() or "unknownSource"
    study_type    = data.get("study_type", "fMRI")
    response_data = data.get("response", {})
    participant_id = response_data.get("participantID", "").strip() or "unknownID"
    formatted_date_str = get_formatted_date(response_data)
    questionnaire = data.get("questionnaire", "unknown")
    config = {
        "order":     data.get("order", []),
        "questions": data.get("questions", {})
    }
    reversal_config = data.get("reverse")

    print(f"  participant : {participant_id}")
    print(f"  source      : {source}")
    print(f"  study_type  : {study_type}")
    print(f"  date        : {formatted_date_str}")
    print(f"  questionnaire: {questionnaire}")

    if questionnaire == "pq16":
        response_data = clean_pq16_data(response_data)

    all_response_data = apply_reversal_if_needed(response_data, reversal_config)

    fieldnames    = config["order"]
    group_row     = fieldnames.copy()
    question_row  = [config["questions"].get(f, f) for f in fieldnames]
    data_row      = [all_response_data.get(f, "") for f in fieldnames]

    # Skip if no meaningful data
    non_empty_fields = [
        f for f in fieldnames
        if f not in ("date", "time", "participantID")
        and str(all_response_data.get(f, "")).strip() != ""
    ]
    if not non_empty_fields:
        print("SKIPPED: No data other than date/time/participantID.")
        return

    SUBFOLDER_SOURCES = {"postScan"}

    session            = get_session()
    requested_folder_id = data.get("box_folder_id")
    folder_id          = ensure_valid_folder_id(session, requested_folder_id)
    entries            = get_folder_entries(session, folder_id)
    root_folder_id     = None

    if entries is None:
        folder_id = DEFAULT_BOX_FOLDER_ID
        entries   = get_folder_entries(session, folder_id)

    if source in SUBFOLDER_SOURCES:
        from main import get_or_create_subfolder
        subfolder_name = f"{participant_id}_{formatted_date_str}"
        root_folder_id = folder_id
        folder_id      = get_or_create_subfolder(session, folder_id, subfolder_name)
        entries        = get_folder_entries(session, folder_id)

    success_count = 0

    if source not in {"feedback"}:
        ok = process_individual_file_upload(
            session, data, entries, participant_id, questionnaire, folder_id,
            group_row, question_row, data_row, source, study_type, formatted_date_str
        )
        if ok:
            success_count += 1
            print("  ✓ Individual file uploaded")
        else:
            print("  ✗ Individual file upload failed/skipped")

    ok = merge_csvs_for_participant(
        session, folder_id, study_type, source, participant_id, formatted_date_str,
        entries, group_row, question_row, data_row, "single_file",
        QUESTIONNAIRE_ORDER, questionnaire, root_folder_id
    )
    if ok:
        success_count += 1
        print("  ✓ Merged CSV updated")

    ok = process_master_file_update(
        session, data, entries, questionnaire, folder_id,
        group_row, question_row, data_row, source, study_type,
        formatted_date_str, participant_id
    )
    if ok:
        success_count += 1
        print("  ✓ Master file updated")

        from main import FIRST_SCREENER_ROOT_FOLDER_ID, update_master_csv
        if source in {"firstScreenerAutistic", "firstScreenerNeurotypical"}:
            try:
                slb_root    = FIRST_SCREENER_ROOT_FOLDER_ID
                slb_entries = get_folder_entries(session, slb_root)
                if update_master_csv(session, group_row, question_row, data_row,
                                     slb_root, "firstScreener", "slb_fMRI",
                                     formatted_date_str, slb_entries, participant_id):
                    print("  ✓ SLB firstScreener master updated")
            except Exception as e:
                print(f"  ✗ SLB master update failed: {e}")

    print(f"\n/webhook done — {success_count} operation(s) succeeded.")


# ── Run /webhook2 logic ──────────────────────────────────────────────────────
def run_webhook2(data):
    print("\n" + "="*60)
    print("Running /webhook2 (Google Sheets stratified sampling)")
    print("="*60)

    from main import update_stratified_doc_screener

    response_data = data.get("response", {})
    final_status  = response_data.get("finalStatus", "").lower()

    if final_status != "complete":
        print(f"SKIPPED: finalStatus is '{final_status}' (must be 'complete').")
        return

    print(f"  finalStatus : {final_status} — proceeding with update...")
    update_stratified_doc_screener(response_data)
    print("  ✓ Stratified sampling spreadsheet updated.")


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    check_token(PAYLOAD)

    run_webhook(PAYLOAD)
    run_webhook2(PAYLOAD)

    print("\nDone.")