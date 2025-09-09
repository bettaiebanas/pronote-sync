import os, hashlib, datetime as dt
from dateutil.tz import gettz
from pronotepy import Client
from pronotepy.ent import ent77
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ===== CONFIG =====
PRONOTE_URL  = "https://0771342r.index-education.net/pronote/parent.html"  # sans ?identifiant=...
PRONOTE_USER = os.getenv("PRONOTE_USER") or ""
PRONOTE_PASS = os.getenv("PRONOTE_PASS") or ""

GOOGLE_CAL_ID = "family15066434840617961429@group.calendar.google.com"  # agenda Famille
TITLE_PREFIX  = "[Mo] "
COLOR_ID      = "6"   # 6=orange (5=jaune, 11=vert, etc.)
LOOK_BACK_DAYS  = 14
LOOK_AHEAD_DAYS = 540
TZ = "Europe/Paris"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

def gcal_service():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

def stable_id(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def list_existing_prefixed(svc, start_iso, end_iso):
    items, page = [], None
    while True:
        res = svc.events().list(
            calendarId=GOOGLE_CAL_ID, timeMin=start_iso, timeMax=end_iso,
            singleEvents=True, showDeleted=False, pageToken=page
        ).execute()
        items += [e for e in res.get("items", []) if e.get("summary","").startswith(TITLE_PREFIX)]
        page = res.get("nextPageToken")
        if not page: break
    return items

def main():
    tz = gettz(TZ)
    now = dt.datetime.now(tz)
    start_win = now - dt.timedelta(days=LOOK_BACK_DAYS)
    end_win   = now + dt.timedelta(days=LOOK_AHEAD_DAYS)

    # 1) Connexion PRONOTE via ENT77
    client = Client(PRONOTE_URL, username=PRONOTE_USER, password=PRONOTE_PASS, ent=ent77)
    if not client.logged_in:
        raise SystemExit("Connexion ENT77/PRONOTE échouée. Vérifie identifiants et URL 'parent.html'.")

    # 2) Service Google Calendar
    svc = gcal_service()

    # 3) Récup semaines PRONOTE
    lessons, d = [], start_win.date()
    while d <= end_win.date():
        lessons += client.lessons(date_from=d, date_to=d + dt.timedelta(days=6))
        d += dt.timedelta(days=7)

    # 4) État désiré
    desired = {}
    for l in lessons:
        if getattr(l, "canceled", False):
            continue
        start = l.start.astimezone(tz)
        end   = l.end.astimezone(tz)
        title = TITLE_PREFIX + (l.subject or "Cours")
        parts = []
        if l.teacher: parts.append(f"Prof: {l.teacher}")
        if getattr(l, "group_name", None): parts.append(f"Groupe: {l.group_name}")
        if getattr(l, "content", None):    parts.append(f"Contenu: {l.content}")
        desc = "\n".join(parts) if parts else ""
        loc  = l.classroom or ""
        key  = f"{start.isoformat()}|{(l.subject or '').strip()}|{loc}|{(l.teacher or '').strip()}"
        ev_id = stable_id(key)
        desired[ev_id] = {
            "id": ev_id,
            "summary": title,
            "location": loc,
            "description": desc,
            "colorId": COLOR_ID,
            "start": {"dateTime": start.isoformat(), "timeZone": TZ},
            "end":   {"dateTime": end.isoformat(),   "timeZone": TZ},
        }

    # 5) Réconciliation (crée/maj/supprime uniquement nos [Mo] dans la fenêtre)
    existing = {
        e["id"]: e
        for e in list_existing_prefixed(svc, start_win.isoformat(), end_win.isoformat())
        if "id" in e
    }
    created = updated = deleted = 0
    for ev_id, body in desired.items():
        if ev_id in existing:
            cur = existing[ev_id]
            changed = (
                cur.get("summary","") != body["summary"] or
                cur.get("location","") != body["location"] or
                cur.get("description","") != body["description"] or
                cur.get("colorId","") != body["colorId"] or
                cur.get("start",{}).get("dateTime") != body["start"]["dateTime"] or
                cur.get("end",{}).get("dateTime")   != body["end"]["dateTime"]
            )
            if changed:
                svc.events().update(calendarId=GOOGLE_CAL_ID, eventId=ev_id, body=body).execute()
                updated += 1
        else:
            svc.events().insert(calendarId=GOOGLE_CAL_ID, body=body).execute()
            created += 1
    for ev_id in list(existing.keys()):
        if ev_id not in desired:
            svc.events().delete(calendarId=GOOGLE_CAL_ID, eventId=ev_id).execute()
            deleted += 1

    print(f"Terminé. créés={created}, maj={updated}, supprimés={deleted}, total_source={len(desired)}")

if __name__ == "__main__":
    main()
