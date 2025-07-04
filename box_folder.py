
import os

def create_participant_folder(participant_id, date_str, subfolder_path):
    """
    Create a folder named {participantID_date} in the specified Box subfolder.
    Args:
        participant_id (str): The participant's ID.
        date_str (str): The date string (e.g., '07-03-2025').
        subfolder_path (str): Relative path under Box root (e.g., 'StudyA/Session1').
    Returns:
        str: The full path to the created folder.
    """
    base_path = r"C:\\Users\\"
    target_dir = os.path.join(base_path, subfolder_path)
    folder_name = f"{participant_id}_{date_str}"
    new_folder_path = os.path.join(target_dir, folder_name)
    os.makedirs(new_folder_path, exist_ok=True)
    return new_folder_path


create_participant_folder("WUH793_01", '07-03-2025', 'My Box Notes')