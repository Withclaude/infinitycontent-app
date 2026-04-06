"""
get_token.py — Run ONCE locally to generate your OAuth2 refresh token.

Steps:
  1. Go to console.cloud.google.com → your project (reelgenerator-490809)
  2. APIs & Services → Credentials → + Create Credentials → OAuth 2.0 Client ID
  3. Application type: Desktop app → Name: "InfinityContent Local" → Create
  4. Download the JSON → save it next to this script as "client_secret.json"
  5. Run:  python3 get_token.py
  6. A browser window opens → log in with the Google account that owns the Drive
  7. Copy the printed values into .streamlit/secrets.toml under [gdrive_oauth]
"""

import json
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]
CLIENT_SECRET_FILE = "client_secret.json"

def main():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")

    print("\n" + "=" * 60)
    print("✅  Authorization successful! Copy these values into")
    print("    .streamlit/secrets.toml under [gdrive_oauth]:")
    print("=" * 60)
    print(f'\n[gdrive_oauth]')
    print(f'client_id     = "{creds.client_id}"')
    print(f'client_secret = "{creds.client_secret}"')
    print(f'refresh_token = "{creds.refresh_token}"')
    print("\n" + "=" * 60)
    print("After adding to secrets.toml, restart the app.")
    print("You can delete client_secret.json and this script.\n")

if __name__ == "__main__":
    main()
