# pronote_playwright_to_family_mo.py
import os, sys, re, hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG =========
ENT_URL      = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL  = os.getenv("PRONOTE_URL", "")       # laisse vide pour passer par la tuile PRONOTE
ENT_USER     = os.getenv("PRONOTE_USER", "")
ENT_PASS     = os.getenv("PRONOTE_PASS", "")

CALENDAR_ID  = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX = "[Mo] "
COLOR_ID     = "6"                                  # 6 = orange
WEEKS_TO_FETCH = 2
HEADFUL      = os.getenv("HEADFUL", "0") == "1"

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

SCREEN_DIR = "screenshots"  # capturas auto (uploadées en artifact par le workflow)

# ========= Google Calendar =========
def get_gcal_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        try:
            from google.auth.transport.requests import Request
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
        except Exception as e:
            print(f"[Google OAuth] {e}")
            raise
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

def stable_event_id(start: datetime, end: datetime, title: str, location: str) -> str:
    base = f"{start.isoformat()}|{end.isoformat()}|{title}|{location}"
    return "mo_" + hashlib.md5(base.encode("utf-8")).hexdigest()

def upsert_event(svc, cal_id: str, ev: Dict[str, Any]) -> str:
    try:
        svc.events().insert(calendarId=cal_id, body=ev, sendUpdates="none").execute()
        return "created"
    except HttpError as e:
        if getattr(e, "resp", None) is not None and getattr(e.resp, "status", None) == 409:
            svc.events().update(calendarId=cal_id, eventId=ev["id"], body=ev, sendUpdates="none").execute()
            return "updated"
        raise

# ========= Parsing =========
HOUR_RE = re.compile(r'(?P<h>\d{1,2})[:hH](?P<m>\d{2})')

def parse_timespan(text: str):
    times = HOUR_RE.findall(text)
    if len(times) >= 2:
        (h1,m1),(h2,m2) = times[0], times[1]
        return (int(h1),int(m1)), (int(h2),int(m2))
    return None

def parse_aria_label(label: str) -> Dict[str, Any]:
    d = {"start": None, "end": None, "summary": None, "room": ""}
    lab = " ".join(label.split())
    tspan = parse_timespan(lab)
    if tspan: d["start"], d["end"] = tspan

    m_room = re.search(r'(?:Salle|Salles?)\s*([A-Za-z0-9\-_. ]+)', lab, re.IGNORECASE)
    if m_room: d["room"] = m_room.group(1).strip()

    summary = lab
    summary = re.sub(r'^\s*\d{1,2}[:hH]\d{2}\s*[–\-]\s*\d{1,2}[:hH]\d{2}\s*', '', summary)
    summary = re.sub(r'(Salle|Salles?).*$', '', summary, flags=re.IGNORECASE)
    summary = re.sub(r'(Prof\.?:.*)$', '', summary, flags=re.IGNORECASE)
    summary = summary.strip(" -–")
    d["summary"] = summary if summary else "Cours"
    return d

def monday_of_week(text_header: str) -> Optional[datetime]:
    m = re.search(r'(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})', text_header)
    if m:
        return datetime.strptime(m.group(1), "%d/%m/%Y")
    return None

def to_datetime(base_monday: Optional[datetime], day_idx: Optional[int], hm: tuple) -> datetime:
    if base_monday is not None and day_idx is not None and 0 <= int(day_idx) <= 6:
        base = base_monday + timedelta(days=int(day_idx))
    else:
        base = datetime.now()
    return base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

# ========= Playwright helpers =========
def first_locator_in_frames(page, selectors: List[str]):
    for frame in page.frames:
        for sel in selectors:
            loc = frame.locator(sel)
            try:
                if loc.count() > 0:
                    return loc.first
            except:
                pass
    return None

def click_first_in_frames(page, selectors: List[str]) -> bool:
    loc = first_locator_in_frames(page, selectors)
    if loc:
        try:
            loc.click()
            return True
        except:
            return False
    return False

def text_exists_any_frame(page, pattern: str) -> bool:
    regex = re.compile(pattern)
    for frame in page.frames:
        try:
            txt = frame.content()
            if regex.search(txt):
                return True
        except:
            pass
    return False

# ========= Navigation =========
def login_ent(page):
    os.makedirs(SCREEN_DIR, exist_ok=True)
    page.goto(ENT_URL, wait_until="load")
    page.wait_for_load_state("networkidle")
    page.screenshot(path=f"{SCREEN_DIR}/01-ent-welcome.png", full_page=True)

    # 1) Cliquer "Se connecter" / "Connexion" / bouton au header si présent
    click_first_in_frames(page, [
        'a:has-text("Se connecter")',
        'a:has-text("Connexion")',
        'button:has-text("Se connecter")',
        'button:has-text("Connexion")',
        'a[href*="login"]',
        'a[href*="auth"]'
    ])
    page.wait_for_load_state("networkidle")
    page.screenshot(path=f"{SCREEN_DIR}/02-ent-after-click-login.png", full_page=True)

    # 2) Chercher un sélecteur de login sur la page OU dans un iframe (CAS / IdP)
    user_candidates = [
        'input[name="email"]',
        'input[name="username"]',
        '#username',
        'input[type="text"][name*="user"]',
        'input[type="text"]',
        'input[type="email"]',
        'input#email',
        'input[name="login"]',
        'input[name="j_username"]'
    ]
    pass_candidates = [
        'input[type="password"][name="password"]',
        '#password',
        'input[type="password"]',
        'input[name="j_password"]'
    ]
    submit_candidates = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Se connecter")',
        'button:has-text("Connexion")',
        'button:has-text("Valider")'
    ]

    user_loc = first_locator_in_frames(page, user_candidates)
    pass_loc = first_locator_in_frames(page, pass_candidates)

    # Certains ENT proposent des tuiles (EduConnect, etc.). Tente un bouton "Compte .../Identifiant..."
    if not user_loc or not pass_loc:
        click_first_in_frames(page, [
            'button:has-text("Identifiant")',
            'a:has-text("Identifiant")',
            'button:has-text("Compte")',
            'a:has-text("Compte")',
            'a:has-text("ENT")'
        ])
        page.wait_for_load_state("networkidle")
        user_loc = first_locator_in_frames(page, user_candidates)
        pass_loc = first_locator_in_frames(page, pass_candidates)

    if not user_loc or not pass_loc:
        page.screenshot(path=f"{SCREEN_DIR}/03-ent-no-fields.png", full_page=True)
        raise RuntimeError("Champ identifiant ENT introuvable (multi-frames). Mets HEADFUL=1 pour voir.")

    try:
        user_loc.fill(ENT_USER)
        pass_loc.fill(ENT_PASS)
    except Exception as e:
        page.screenshot(path=f"{SCREEN_DIR}/04-ent-fill-error.png", full_page=True)
        raise RuntimeError(f"Impossible de renseigner les champs ENT: {e}")

    # Cliquer submit
    if not click_first_in_frames(page, submit_candidates):
        # tente Enter dans le frame des inputs
        try:
            user_loc.press("Enter")
        except:
            pass

    page.wait_for_load_state("networkidle")
    page.screenshot(path=f"{SCREEN_DIR}/05-ent-after-submit.png", full_page=True)

def open_pronote(context, page):
    if PRONOTE_URL:
        page.goto(PRONOTE_URL, wait_until="load")
        page.wait_for_load_state("networkidle")
        page.screenshot(path=f"{SCREEN_DIR}/06-pronote-direct.png", full_page=True)
        return page

    # Chercher tuile / lien PRONOTE (nouvel onglet ou même onglet)
    with page.expect_popup() as p:
        clicked = click_first_in_frames(page, [
            'a:has-text("PRONOTE")',
            'a[title*="PRONOTE"]',
            'a[href*="pronote"]',
            'text=PRONOTE'
        ])
        if not clicked:
            page.screenshot(path=f"{SCREEN_DIR}/06-pronote-tile-not-found.png", full_page=True)
            raise RuntimeError("Tuile/lien PRONOTE introuvable après login ENT.")
    try:
        pronote_page = p.value
        pronote_page.wait_for_load_state("networkidle")
    except PWTimeout:
        pronote_page = page
        pronote_page.wait_for_load_state("networkidle")

    pronote_page.screenshot(path=f"{SCREEN_DIR}/07-pronote-home.png", full_page=True)
    return pronote_page

def goto_timetable(pronote_page):
    # Clique "Vie scolaire" si présent
    click_first_in_frames(pronote_page, [
        'text="Vie scolaire"',
        'button:has-text("Vie scolaire")',
        'a:has-text("Vie scolaire")'
    ])
    pronote_page.wait_for_load_state("networkidle")
    pronote_page.wait_for_timeout(500)
    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-pronote-vie-scolaire.png", full_page=True)

def extract_week_info(pronote_page) -> Dict[str, Any]:
    # Header semaine
    header_text = ""
    for sel in ['text=/Semaine .* au .*/', '.titrePeriode', '.zoneSemaines', 'header']:
        loc = first_locator_in_frames(pronote_page, [sel])
        if loc:
            try:
                header_text = loc.inner_text()
                if header_text:
                    break
            except:
                pass

    d0 = monday_of_week(header_text)

    # Cases: aria-label / title / texte
    tiles = pronote_page.evaluate("""
    () => {
      const out = [];
      const add = (el, label) => {
        if (!label) return;
        let dayIndex = null, p = el;
        while (p) {
          if (p.getAttribute && p.getAttribute('data-dayindex')) { dayIndex = parseInt(p.getAttribute('data-dayindex')); break; }
          p = p.parentElement;
        }
        out.push({ label, dayIndex });
      };

      document.querySelectorAll('[aria-label]').forEach(e => {
        const v = e.getAttribute('aria-label');
        if (v && /\d{1,2}[:hH]\d{2}.*\d{1,2}[:hH]\d{2}/.test(v)) add(e, v);
      });
      document.querySelectorAll('[title]').forEach(e => {
        const v = e.getAttribute('title');
        if (v && /\d{1,2}[:hH]\d{2}.*\d{1,2}[:hH]\d{2}/.test(v)) add(e, v);
      });
      document.querySelectorAll('*').forEach(e => {
        const t = (e.innerText || '').trim();
        if (t && /\d{1,2}[:hH]\d{2}.*\d{1,2}[:hH]\d{2}/.test(t) && t.length < 160) add(e, t);
      });
      return out;
    }
    """)
    return {"monday": d0, "tiles": tiles, "header": header_text}

def iter_next_week(pronote_page) -> bool:
    if click_first_in_frames(pronote_page, [
        'button[title*="suivante"]',
        'button[aria-label*="suivante"]',
        'button:has-text("→")',
        'a[title*="suivante"]',
        'a:has-text("Semaine suivante")'
    ]):
        pronote_page.wait_for_load_state("networkidle")
        pronote_page.wait_for_timeout(500)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/09-pronote-next-week.png", full_page=True)
        return True
    return False

# ========= Main =========
def run():
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("Identifiants ENT manquants: PRONOTE_USER / PRONOTE_PASS.")

    svc = get_gcal_service()
    created = updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = context.new_page()

        # ENT
        login_ent(page)

        # PRONOTE
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # Parcours n semaines
        for w in range(WEEKS_TO_FETCH):
            info = extract_week_info(pronote)
            d0 = info["monday"]
            tiles = info["tiles"] or []
            print(f"Semaine {w+1}: {len(tiles)} cases, header='{(info.get('header') or '')[:80]}'")

            for t in tiles:
                label = (t.get("label") or "").strip()
                if not label:
                    continue
                parsed = parse_aria_label(label)
                if not parsed["start"] or not parsed["end"]:
                    continue

                start_dt = to_datetime(d0, t.get("dayIndex"), parsed["start"])
                end_dt   = to_datetime(d0, t.get("dayIndex"), parsed["end"])

                now = datetime.now()
                if end_dt < (now - timedelta(days=21)) or start_dt > (now + timedelta(days=90)):
                    continue

                title = (parsed["summary"] or "Cours").strip()
                title = f"{TITLE_PREFIX}{title}"

                ev = {
                    "id": stable_event_id(start_dt, end_dt, title, parsed["room"]),
                    "summary": title,
                    "location": parsed["room"],
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                }
                try:
                    action = upsert_event(svc, CALENDAR_ID, ev)
                    if action == "created": created += 1
                    else: updated += 1
                except HttpError as e:
                    print(f"[GCAL] {e}")

            if w < WEEKS_TO_FETCH - 1 and not iter_next_week(pronote):
                break

        browser.close()

    print(f"Terminé. créés={created}, maj={updated}")

if __name__ == "__main__":
    try:
        run()
    except Exception as ex:
        try:
            os.makedirs(SCREEN_DIR, exist_ok=True)
        except: pass
        print(f"[FATAL] {ex}")
        sys.exit(1)
