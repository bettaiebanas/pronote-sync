# gcal_strip_prefix.py
from __future__ import annotations
import os, re, time, random
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

CALENDAR_ID = os.getenv("CALENDAR_ID", "")              # <-- ton agenda
PREFIX_RX   = os.getenv("PREFIX_REGEX", r"\s*\[Mo\]\s*") # regex à retirer partout
ONLY_SOURCE = os.getenv("ONLY_SOURCE", "1") == "1"       # 1 = ne toucher qu'aux évts créés par ce script
DRY_RUN     = os.getenv("DRY_RUN", "0") == "1"           # 1 = simulateur (aucune écriture)
TIME_MIN    = os.getenv("TIME_MIN", "2000-01-01T00:00:00Z")
TIME_MAX    = os.getenv("TIME_MAX", "2100-01-01T00:00:00Z")

def gsvc():
    creds=None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        from google.auth.transport.requests import Request
        if creds and getattr(creds,"expired", False) and getattr(creds,"refresh_token", None):
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as f: f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

def main():
    if not CALENDAR_ID:
        raise SystemExit("CALENDAR_ID manquant")
    svc = gsvc()

    rx = re.compile(PREFIX_RX, re.I)
    total = changed = 0
    page_token = None

    while True:
        params = dict(
            calendarId=CALENDAR_ID,
            timeMin=TIME_MIN,
            timeMax=TIME_MAX,
            singleEvents=True,
            showDeleted=False,
            maxResults=250
        )
        if ONLY_SOURCE:
            params["privateExtendedProperty"] = "source=pronote_playwright"
        if page_token:
            params["pageToken"] = page_token

        resp = svc.events().list(**params).execute()
        for ev in resp.get("items", []):
            total += 1
            old = ev.get("summary", "") or ""
            new = rx.sub(" ", old)
            new = re.sub(r"\s{2,}", " ", new).strip()
            if new != old:
                changed += 1
                if DRY_RUN:
                    print(f"DRY: {ev['id']}  '{old}'  ->  '{new}'")
                else:
                    tries = 0
                    while True:
                        try:
                            svc.events().patch(
                                calendarId=CALENDAR_ID,
                                eventId=ev["id"],
                                body={"summary": new},
                                sendUpdates="none"
                            ).execute()
                            break
                        except HttpError as e:
                            if getattr(e, "res", None) and e.res.status in (403, 429):
                                time.sleep(min(30, (2**tries) + random.uniform(0,0.5))); tries += 1
                                continue
                            raise

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    print(f"Scannés: {total} — Modifiés: {changed} — DRY_RUN={DRY_RUN}")

if __name__ == "__main__":
    main()
