# pronote_playwright_to_family_mo.py
import os, sys, re, hashlib, unicodedata, time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG (via secrets/env GitHub) =========
ENT_URL     = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL = os.getenv("PRONOTE_URL", "")
ENT_USER    = os.getenv("PRONOTE_USER", "")
ENT_PASS    = os.getenv("PRONOTE_PASS", "")

# Sélecteurs pour atteindre l’EDT
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()  # ex: "parent.html"

# Onglets semaine j_1, j_2, …
WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()  # ex: #GInterface\.Instances\[2\]\.Instances\[0\]_j_{n}
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))

WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

# Google Calendar
CALENDAR_ID  = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX = os.getenv("TITLE_PREFIX", "[Mo] ")
COLOR_ID     = os.getenv("COLOR_ID", "6")  # 6 = orange

# OAuth
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts (ms)
TIMEOUT_MS = 120_000
SCREEN_DIR = "screenshots"

# --------- Utilitaires GCal ----------
def get_gcal_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        try:
            from google.auth.transport.requests import Request
            if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
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

def make_hash_id(start: datetime, end: datetime, title: str, location: str) -> str:
    base = f"{start.isoformat()}|{end.isoformat()}|{title}|{location}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def find_event_by_hash(svc, cal_id: str, h: str):
    try:
        resp = svc.events().list(
            calendarId=cal_id,
            privateExtendedProperty=f"mo_hash={h}",
            singleEvents=True,
            maxResults=1,
            orderBy="startTime",
        ).execute()
        items = resp.get("items", [])
        return items[0] if items else None
    except HttpError as e:
        print(f"[GCAL:list] {e}")
        return None

def upsert_event_by_hash(svc, cal_id: str, h: str, body: Dict[str, Any]) -> str:
    existing = find_event_by_hash(svc, cal_id, h)
    try:
        if existing:
            ev_id = existing["id"]
            svc.events().update(calendarId=cal_id, eventId=ev_id, body=body, sendUpdates="none").execute()
            return "updated"
        else:
            svc.events().insert(calendarId=cal_id, body=body, sendUpdates="none").execute()
            return "created"
    except HttpError as e:
        print(f"[GCAL:upsert] {e}")
        return "error"

# --------- Parsing ----------
MONTHS_FR = {
    "janvier":1, "février":2, "fevrier":2, "mars":3, "avril":4, "mai":5, "juin":6,
    "juillet":7, "août":8, "aout":8, "septembre":9, "octobre":10, "novembre":11, "décembre":12, "decembre":12,
}

HOUR_RE = re.compile(r"(\d{1,2})\s*(?:h|heures|:)\s*(\d{2})", re.I)

def _clean_title(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "").strip()
    # retire balises PRONOTE qui ne doivent pas polluer le titre
    s = re.sub(r"^\s*(Prof\.?\s*absent|Cours annulé|Changement de salle)\s*[:\-–]?\s*", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s)
    return s or "Cours"

def parse_fr_datetime(day: int, month_name: str, year: int, h: int, m: int) -> Optional[datetime]:
    mnum = MONTHS_FR.get(month_name.lower())
    if not mnum:
        return None
    return datetime(year, mnum, day, int(h), int(m), 0)

def parse_aria_label(label: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Exemples:
      'Cours du 8 septembre 2025 de 9 heures 05 à 10 heures 00'
      'Cours du 11 sept. 2025 de 09:05 à 10:00' (on gère large)
    """
    txt = " ".join((label or "").split())
    # date du ... 8 septembre 2025 ...
    mdate = re.search(r"du\s+(\d{1,2})\s+([A-Za-zéèêûîôàç\.]+)\s+(\d{4})", txt, re.I)
    # heures
    hours = HOUR_RE.findall(txt)
    if not mdate or len(hours) < 2:
        return None
    d, mname, y = int(mdate.group(1)), mdate.group(2).replace(".", ""), int(mdate.group(3))
    (h1,m1), (h2,m2) = hours[0], hours[1]
    start = parse_fr_datetime(d, mname, y, int(h1), int(m1))
    end   = parse_fr_datetime(d, mname, y, int(h2), int(m2))
    return (start, end) if start and end else None

# --------- Playwright helpers ----------
def first_locator_in_frames(page, selectors: List[str]):
    for frame in page.frames:
        for sel in selectors:
            try:
                loc = frame.locator(sel)
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

def accept_cookies_any(page):
    texts = ["Tout accepter","Accepter tout","J'accepte","Accepter","OK","Continuer","J’ai compris","J'ai compris"]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_in_frames(page, sels)

def click_css_in_frames(page, css: str, frame_url_contains: str = "", screenshot_tag: str = "") -> bool:
    if not css:
        return False
    for fr in page.frames:
        if frame_url_contains and frame_url_contains not in fr.url:
            continue
        try:
            loc = fr.locator(css)
            if loc.count() > 0:
                loc.first.click()
                page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                if screenshot_tag:
                    try: page.screenshot(path=f"{SCREEN_DIR}/08-clicked-{screenshot_tag}.png", full_page=True)
                    except: pass
                return True
        except Exception as e:
            print(f"[NAV] click_css_in_frames fail in {fr.url}: {e}")
    return False

def login_ent(page):
    os.makedirs(SCREEN_DIR, exist_ok=True)
    page.set_default_timeout(TIMEOUT_MS)
    page.goto(ENT_URL, wait_until="load")
    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    page.screenshot(path=f"{SCREEN_DIR}/01-ent-welcome.png", full_page=True)

    click_first_in_frames(page, [
        'a:has-text("Se connecter")','a:has-text("Connexion")',
        'button:has-text("Se connecter")','button:has-text("Connexion")',
        'a[href*="login"]','a[href*="auth"]'
    ])
    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    page.screenshot(path=f"{SCREEN_DIR}/02-ent-after-click-login.png", full_page=True)

    user_candidates = [
        'input[name="email"]','input[name="username"]','#username',
        'input[type="text"][name*="user"]','input[type="text"]','input[type="email"]',
        'input#email','input[name="login"]','input[name="j_username"]'
    ]
    pass_candidates = [
        'input[type="password"][name="password"]','#password','input[type="password"]','input[name="j_password"]'
    ]
    submit_candidates = [
        'button[type="submit"]','input[type="submit"]',
        'button:has-text("Se connecter")','button:has-text("Connexion")','button:has-text("Valider")'
    ]

    user_loc = first_locator_in_frames(page, user_candidates)
    pass_loc = first_locator_in_frames(page, pass_candidates)
    if not user_loc or not pass_loc:
        click_first_in_frames(page, [
            'button:has-text("Identifiant")','a:has-text("Identifiant")',
            'button:has-text("Compte")','a:has-text("Compte")','a:has-text("ENT")'
        ])
        page.wait_for_load_state("domcontentloaded")
        accept_cookies_any(page)
        user_loc = first_locator_in_frames(page, user_candidates)
        pass_loc = first_locator_in_frames(page, pass_candidates)

    if not user_loc or not pass_loc:
        page.screenshot(path=f"{SCREEN_DIR}/03-ent-no-fields.png", full_page=True)
        raise RuntimeError("Champ identifiant ENT introuvable. Mets HEADFUL=1 pour ajuster.")

    user_loc.fill(ENT_USER)
    pass_loc.fill(ENT_PASS)
    if not click_first_in_frames(page, submit_candidates):
        user_loc.press("Enter")

    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    page.screenshot(path=f"{SCREEN_DIR}/05-ent-after-submit.png", full_page=True)

def open_pronote(context, page):
    page.set_default_timeout(TIMEOUT_MS)
    if PRONOTE_URL:
        page.goto(PRONOTE_URL, wait_until="load")
        page.wait_for_load_state("domcontentloaded")
        accept_cookies_any(page)
        page.screenshot(path=f"{SCREEN_DIR}/06-pronote-direct.png", full_page=True)
        return page

    with page.expect_popup() as p:
        clicked = click_first_in_frames(page, [
            'a:has-text("PRONOTE")','a[title*="PRONOTE"]','a[href*="pronote"]','text=PRONOTE'
        ])
        if not clicked:
            page.screenshot(path=f"{SCREEN_DIR}/06-pronote-tile-not-found.png", full_page=True)
            raise RuntimeError("Tuile PRONOTE introuvable après login ENT.")
    try:
        pronote_page = p.value
        pronote_page.wait_for_load_state("domcontentloaded")
    except PWTimeout:
        pronote_page = page
        pronote_page.wait_for_load_state("domcontentloaded")

    accept_cookies_any(pronote_page)
    pronote_page.screenshot(path=f"{SCREEN_DIR}/07-pronote-home.png", full_page=True)
    return pronote_page

def goto_timetable(pronote_page):
    pronote_page.set_default_timeout(TIMEOUT_MS)
    accept_cookies_any(pronote_page)

    # Chemin explicite fourni ?
    if TIMETABLE_PRE_SELECTOR:
        click_css_in_frames(pronote_page, TIMETABLE_PRE_SELECTOR, TIMETABLE_FRAME, "pre-selector")
    if TIMETABLE_SELECTOR:
        if click_css_in_frames(pronote_page, TIMETABLE_SELECTOR, TIMETABLE_FRAME, "timetable-selector"):
            return

    # Fallback : clic par libellé
    attempts = [
        ["Emploi du temps", "Mon emploi du temps", "Emplois du temps"],
        ["Planning", "Agenda"],
        ["Vie scolaire", "Emploi du temps"],
    ]
    for pats in attempts:
        for pat in pats:
            if click_first_in_frames(pronote_page, [
                f'role=link[name=/{pat}/i]', f'role=button[name=/{pat}/i]',
                f'a:has-text("{pat}")', f'button:has-text("{pat}")'
            ]):
                pronote_page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                return

def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        click_first_in_frames(page, ['button:has-text("Tout voir")','a:has-text("Tout voir")'])
        page.wait_for_timeout(400)

def goto_week_by_index(page, n: int) -> bool:
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)
    ok = click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}")
    return ok

# --------- Extraction depuis la grille PRONOTE ----------
def extract_entries_for_week(page) -> List[Dict[str, Any]]:
    """
    Retourne une liste d'entrées brutes:
      { 'start': datetime, 'end': datetime, 'summary': 'FRANCAIS', 'room': 'S11', 'raw': '...' }
    """
    js = r"""
    () => {
      const out = [];
      // chaque bloc cours avec aria-label et le petit tableau contigu
      const blocks = Array.from(document.querySelectorAll('div[id*="_coursInt_"][aria-label]'));
      for (const b of blocks) {
        const label = b.getAttribute('aria-label') || '';
        // le mini tableau d'infos (matière, prof, salle) est proche:
        // on remonte puis cherche le premier td[id*="_cont"] dans le même "cours"
        let td = b.closest('div[id*="_cours_"]')?.querySelector('td[id*="_cont"]');
        if (!td) {
          // secours : prendre le td le plus proche en remontant
          td = b.parentElement?.querySelector('td[id*="_cont"]') || null;
        }
        let infos = [];
        if (td) {
          infos = Array.from(td.querySelectorAll('.NoWrap.AlignementMilieu')).map(x => (x.innerText || '').trim()).filter(Boolean);
        }
        out.push({ label, infos });
      }
      return out;
    }
    """
    entries = []
    for fr in page.frames:
        try:
            blocks = fr.evaluate(js)
        except Exception:
            continue
        for blk in (blocks or []):
            label = blk.get("label") or ""
            parsed = parse_aria_label(label)
            if not parsed:
                continue
            start, end = parsed

            # Infos : typiquement [ 'PHYSIQUE-CHIMIE', 'LEHMANN F.', '[4GPI]', '004' ]
            infos = blk.get("infos") or []
            # matière = première ligne "textuelle" un peu longue
            summary = None
            for s in infos:
                if len(s) >= 3 and not re.match(r"^\[[^\]]+\]$", s):
                    summary = s
                    break
            if not summary and infos:
                summary = infos[0]
            summary = _clean_title(summary or "Cours")

            # salle = dernière ligne qui ressemble à salle (alphanum court)
            room = ""
            for s in reversed(infos):
                if re.match(r"^[A-Za-z0-9\-/_]{2,6}$", s):
                    room = s
                    break

            entries.append({
                "start": start, "end": end,
                "summary": summary, "room": room,
                "raw": label
            })
    return entries

def merge_adjacent(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fusionne les segments contigus (même résumé + même salle + même jour) avec un écart <= 6 minutes.
    """
    if not entries:
        return []
    # tri par start
    entries = sorted(entries, key=lambda e: e["start"])
    out = [entries[0]]
    for e in entries[1:]:
        last = out[-1]
        same_day = last["start"].date() == e["start"].date()
        if same_day and last["summary"] == e["summary"] and last["room"] == e["room"]:
            gap = (e["start"] - last["end"]).total_seconds() / 60.0
            if -1 <= gap <= 6:  # chevauchement léger ou collé
                last["end"] = max(last["end"], e["end"])
                continue
        out.append(e)
    return out

# --------- RUN ----------
def run():
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("Identifiants ENT manquants: PRONOTE_USER / PRONOTE_PASS.")

    svc = get_gcal_service()

    # petit check de présence du calendrier visé
    try:
        cl = svc.calendarList().list().execute()
        total = len(cl.get("items", []))
        present = any((it.get("id") == CALENDAR_ID) for it in cl.get("items", []))
        print(f"[DBG] CalendarList loaded: {total} calendars. CALENDAR_ID present? {present}")
    except Exception as e:
        print(f"[DBG] calendarList error: {e}")

    created = updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=os.getenv("HEADFUL","0")!="1", args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = context.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        login_ent(page)
        pronote = open_pronote(context, page)
        goto_timetable(pronote)
        ensure_all_visible(pronote)

        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1
        print(f"[CFG] Weeks: {start_idx}..{end_idx}")

        for week_idx in range(start_idx, end_idx + 1):
            goto_week_by_index(pronote, week_idx)
            ensure_all_visible(pronote)

            raw = extract_entries_for_week(pronote)
            print(f"Semaine {week_idx}: {len(raw)} cours, header=''")
            merged = merge_adjacent(raw)
            print(f"[DBG]   entries construits: {len(raw)}")
            print(f"[DBG]   après fusion: {len(merged)}")

            for e in merged:
                start_dt: datetime = e["start"]
                end_dt:   datetime = e["end"]

                # fenêtre de sécurité (–21j .. +90j)
                now = datetime.now()
                if end_dt < (now - timedelta(days=21)) or start_dt > (now + timedelta(days=90)):
                    continue

                title = f"{TITLE_PREFIX}{e['summary']}"
                hash_id = make_hash_id(start_dt, end_dt, title, e["room"])

                body = {
                    "summary": title,
                    "location": e["room"],
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"mo_hash": hash_id, "source": "pronote_playwright"}},
                    "description": e.get("raw",""),
                }

                act = upsert_event_by_hash(svc, CALENDAR_ID, hash_id, body)
                if act == "created": created += 1
                elif act == "updated": updated += 1

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
