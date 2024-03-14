import os
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import prefect as pf
from prefect.blocks.system import Secret

secret_block_google_share_drive = Secret.load("google-share-drive")


# Define the Google Drive API scopes and service account file path
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "/mnt/c/Users/Sleazy/Desktop/Projects/PrefectDeploymentDocker/credentials/GoogleApiKey.json"


# Access the stored secret

SERVICE_ACCOUNT_FILE = secret_block_google_share_drive.get()

# Create credentials using the service account file
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Build the Google Drive service
drive_service = build('drive', 'v3', credentials=credentials)

def create_folder(folder_name, parent_folder_id=None):
    """Create a folder in Google Drive and return its ID."""
    try:
        folder_metadata = {
            'name': folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            'parents': [parent_folder_id] if parent_folder_id else []
        }

        created_folder = drive_service.files().create(
            body=folder_metadata,
            fields='id'
        ).execute()

        print(f'Created Folder ID: {created_folder["id"]}')
        return created_folder["id"]
    except HttpError as error:
            print(f'An error occurred: {error}')

def list_folder(parent_folder_id=None):
    """List folders and files in Google Drive."""
    results = drive_service.files().list(
        q=f"'{parent_folder_id}' in parents and trashed=false" if parent_folder_id else None,
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType)"
    ).execute()
    items = results.get('files', [])

    if not items:
        print("No folders or files found in Google Drive.")
    else:
        print("Folders and files in Google Drive:")
        for item in items:
            print(f"Name: {item['name']}, ID: {item['id']}, Type: {item['mimeType']}")

def list_files(parent_folder_id=None, _print=False):
    """List folders and files in Google Drive."""
    results = drive_service.files().list(
        q=f"'{parent_folder_id}' in parents and trashed=false" if parent_folder_id else None,
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType, modifiedTime)"
    ).execute()
    items = results.get('files', [])
    if _print:
        if not items:
            print("No folders or files found in Google Drive.")
        else:
            print("Folders and files in Google Drive:")
            for item in items:
                print(f"Name: {item['name']}, ID: {item['id']}, Type: {item['mimeType']}, Modified Time: {item['modifiedTime']}")
    # convert items to a pandas dataframe
    return pd.DataFrame(items)      

def delete_files(file_or_folder_id):
    """Delete a file or folder in Google Drive by ID."""
    try:
        drive_service.files().delete(fileId=file_or_folder_id).execute()
        print(f"Successfully deleted file/folder with ID: {file_or_folder_id}")
    except HttpError as error:
        print(f"Error deleting file/folder with ID: {file_or_folder_id}")
        print(f'An error occurred: {error}')

def download_file(file_id, destination_path):
    """Download a file from Google Drive by its ID."""
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(destination_path, mode='wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")

def delete_folder(folder_name):
    try:
        # Call the Drive v3 API to get the folder ID
        results = drive_service.files().list(
            q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'",
            fields='files(id,name)').execute()
        items = results.get('files', [])

        if not items:
            print('No folder found.')
        else:
            print('Folders:')
            for item in items:
                print(u'{0} ({1})'.format(item['name'], item['id']))
                # Delete the folder
                drive_service.files().delete(fileId=item['id']).execute()

    except HttpError as error:
        print(f'An error occurred: {error}')

@pf.task(name="[gdrive] uppload")
def upload_file(file_path, folder_parent_id):
    try:
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_parent_id]
        }
        media = MediaFileUpload(file_path, resumable=True)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f'File ID: {file.get("id")}')
        return file.get("id")
    except HttpError as error:
        print(f'An error occurred: {error}')

def get_file_url(file_id):
    try:
        file = drive_service.files().get(fileId=file_id, fields='webViewLink').execute()
        print(f'File URL: {file.get("webViewLink")}')
    except HttpError as error:
        print(f'An error occurred: {error}')

def get_file_shared_user_list(file_id):
    try:
        permissions = drive_service.permissions().list(fileId=file_id).execute()
        print('Shared users:')
        for permission in permissions.get('permissions', []):
            print(f'{permission.get("id")}: {permission.get("emailAddress")}')
    except HttpError as error:
        print(f'An error occurred: {error}')

def update_file_shared_user_list(file_id, email, role='reader', type='user'):
    try:
        permission = {
            'type': type,
            'role': role,
            'emailAddress': email
        }
        drive_service.permissions().create(fileId=file_id, body=permission).execute()
        print(f'File shared with: {email}')
    except HttpError as error:
        print(f'An error occurred: {error}')

def get_file_path_directory_in_drive(file_id):
    try:
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        print(f'File Path: {file.get("parents")}')
    except HttpError as error:
        print(f'An error occurred: {error}')

def get_file_owner(file_id):
    try:
        file = drive_service.files().get(fileId=file_id, fields='owners').execute()
        print(f'File Owner: {file.get("owners")}')
    except HttpError as error:
        print(f'An error occurred: {error}')

def get_file_info(file_id):
    try:
        file = drive_service.files().get(fileId=file_id).execute()
        print(f'File Info: {file}')
    except HttpError as error:
        print(f'An error occurred: {error}')