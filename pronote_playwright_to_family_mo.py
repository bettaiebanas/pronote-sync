# -*- coding: utf-8 -*-
import os, sys, re, hashlib, time, unicodedata
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= ENV / CONFIG =========
ENT_URL       = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL   = os.getenv("PRONOTE_URL", "")
ENT_USER      = os.getenv("PRONOTE_USER", "")
ENT_PASS      = os.getenv("PRONOTE_PASS", "")

# tes sélecteurs (déjà utilisés dans ton workflow)
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()   # ex: "#GInterface\\.Instances\\[0\\]\\.Instances\\[1\\]_Combo5"
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()       # ex: "#GInterface\\.Instances\\[0\\]\\.Instances\\[1\\]_Liste_niveau5 > ul > li:nth-child(1) > div > div"
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()          # ex: "parent.html"

WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "#GInterface\\.Instances\\[2\\]\\.Instances\\[0\\]_j_{n}")
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))
WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

# Google Calendar
CALENDAR_ID   = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX  = "[Mo] "
COLOR_ID      = "6"
HEADFUL       = os.getenv("HEADFUL", "0") == "1"

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts & sorties
TIMEOUT_MS  = 120_000
SCREEN_DIR  = "screenshots"

# ========= Google Calendar utils =========
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

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def make_event_id(start: datetime, end: datetime, title: str, location: str) -> str:
    # ID stable, compatible Google: uniquement a-v et 0-9
    key = f"{start.isoformat()}|{end.isoformat()}|{_norm(title)}|{_norm(location)}"
    return "prn" + hashlib.sha1(key.encode()).hexdigest()[:24]  # <-- plus de "_"


from googleapiclient.errors import HttpError

def upsert_event_by_id(svc, cal_id: str, event_id: str, body: Dict[str, Any]) -> str:
    # Toujours mettre l'id dans le body (insert ne prend PAS eventId=)
    body = dict(body)           # on copie pour ne pas modifier l'original
    body["id"] = event_id

    try:
        # Existe ? => patch
        svc.events().get(calendarId=cal_id, eventId=event_id).execute()
        svc.events().patch(
            calendarId=cal_id, eventId=event_id, body=body, sendUpdates="none"
        ).execute()
        return "updated"
    except HttpError as e:
        # N'existe pas => insert (avec body["id"])
        if getattr(e, "resp", None) and e.resp.status == 404:
            svc.events().insert(
                calendarId=cal_id, body=body, sendUpdates="none"
            ).execute()
            return "created"
        raise


# ========= Parsing helpers =========
FR_MONTHS = {
    "janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"août":8,"aout":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,"decembre":12
}
HOUR_HHMM = re.compile(r"(\d{1,2})\s*[:hH]\s*(\d{2})")

def parse_french_time_hhmm(s: str) -> Optional[Tuple[int,int]]:
    m = HOUR_HHMM.search(s)
    if not m: return None
    h, mnt = int(m.group(1)), int(m.group(2))
    return (h, mnt)

def parse_week_header_dates(txt: str) -> Optional[Tuple[date, date]]:
    # ex: "du 08/09/2025 au 12/09/2025" ou "Semaine B, du 08/09/2025 au 12/09/2025"
    m = re.search(r'(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})', (txt or ""))
    if not m: return None
    d1 = datetime.strptime(m.group(1), "%d/%m/%Y").date()
    d2 = datetime.strptime(m.group(2), "%d/%m/%Y").date()
    return d1, d2

def parse_cours_aria(aria: str, fallback_year: int) -> Optional[Tuple[date, Tuple[int,int], Tuple[int,int]]]:
    """
    aria ex: 'Cours du 8 septembre de 9 heures 05 à 10 heures 00'
    """
    if not aria: return None
    m = re.search(r"du\s+(\d{1,2})\s+([a-zéû]+)\s+de\s+(\d{1,2})\s*heures?\s*(\d{2})\s*à\s*(\d{1,2})\s*heures?\s*(\d{2})", aria, re.I)
    if not m: return None
    day = int(m.group(1))
    month_name = _norm(m.group(2)).replace("û","u")
    month = FR_MONTHS.get(month_name)
    if not month: return None
    h1, m1 = int(m.group(3)), int(m.group(4))
    h2, m2 = int(m.group(5)), int(m.group(6))
    try:
        d = date(fallback_year, month, day)
    except ValueError:
        return None
    return d, (h1, m1), (h2, m2)

def smart_summary_and_room(inner_text: str) -> Tuple[str, str]:
    """
    Texte du bloc (ex: 'PHYSIQUE-CHIMIE\\nLEHMANN F.\\n[4GP1]\\n004').
    On prend la 1ère ligne comme matière, la dernière ressemblant à une salle.
    """
    lines = [l.strip() for l in (inner_text or "").splitlines() if l.strip()]
    title = lines[0] if lines else "Cours"
    room = ""
    if lines:
        cand = lines[-1]
        if re.match(r"^[A-Za-z0-9\-_/]{2,}$", cand):
            room = cand
    return title, room

# ========= Playwright helpers =========
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
    texts = ["Tout accepter","Accepter tout","J'accepte","Accepter",
             "OK","Continuer","J’ai compris","J'ai compris"]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_in_frames(page, sels)

def click_css_in_frames(page, css: str, frame_url_contains: str = "", tag: str = "") -> bool:
    if not css: return False
    for fr in page.frames:
        if frame_url_contains and frame_url_contains not in fr.url:
            continue
        try:
            # scroll into view puis click (Pronote masque parfois hors viewport)
            ok = fr.evaluate(
                """
                (sel) => {
                  const el = document.querySelector(sel);
                  if (!el) return false;
                  try { el.scrollIntoView({block:'center'}); } catch(e) {}
                  el.click();
                  return true;
                }
                """,
                css
            )
            if ok:
                try: page.screenshot(path=f"{SCREEN_DIR}/08-clicked-{tag or 'css'}.png", full_page=True)
                except: pass
                page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                return True
        except Exception as e:
            print(f"[NAV] fail {css} in {fr.url}: {e}")
    return False

# ========= Navigation =========
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
        raise RuntimeError("Champ identifiant ENT introuvable.")

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
    # chemin custom (Vie scolaire -> Emploi du temps)
    if TIMETABLE_PRE_SELECTOR:
        click_css_in_frames(pronote_page, TIMETABLE_PRE_SELECTOR, TIMETABLE_FRAME, "pre-selector")
    if TIMETABLE_SELECTOR:
        if click_css_in_frames(pronote_page, TIMETABLE_SELECTOR, TIMETABLE_FRAME, "timetable-selector"):
            return
    # heuristiques (fallback)
    for pat in [["Emploi du temps","Mon emploi du temps"],["Planning","Agenda"],["Vie scolaire","Emploi du temps"]]:
        for s in pat:
            if click_first_in_frames(pronote_page, [f'a:has-text("{s}")', f'button:has-text("{s}")', f'role=link[name="{s}"]']):
                pronote_page.wait_for_timeout(400)
                return

def goto_week_by_index(page, n: int) -> bool:
    css = (WEEK_TAB_TEMPLATE or "").format(n=n)
    if not css: return False
    ok = click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}")
    if ok:
        try:
            page.wait_for_timeout(300)
        except: pass
    return ok

# ========= Extraction =========
def read_week_header_and_year(page) -> Tuple[str, int]:
    """
    Lit l'entête 'du 08/09/2025 au 12/09/2025' (dans n'importe quel frame).
    Retourne (texte, année).
    """
    header = ""
    for fr in page.frames:
        try:
            txt = fr.evaluate("() => (document.body.innerText||'').replace(/\\s+/g,' ')")
            m = re.search(r'(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})', txt)
            if m:
                header = m.group(0)
                yr = int(m.group(1)[6:10])
                return header, yr
        except:
            pass
    # fallback -> année courante
    return header, datetime.now().year

def extract_courses_for_week(page, fallback_year: int) -> List[Dict[str, Any]]:
    """
    On ne prend que les 'cours' visibles : div[id*="_cours_"][role=listitem]
    Pour chaque élément :
      - ariaLabel (pour jour + heures)
      - innerText (pour matière/salle)
    """
    items = []
    for fr in page.frames:
        try:
            blocks = fr.eval_on_selector_all(
                'div[id*="_cours_"][role="listitem"]',
                """els => els.map(el => {
                    const sin = el.querySelector('.cours-simple');
                    const aria = (sin && sin.getAttribute('aria-label')) || el.getAttribute('aria-label') || '';
                    const txt  = el.innerText || '';
                    return {aria, txt};
                })"""
            )
            if blocks:
                for b in blocks:
                    parsed = parse_cours_aria(b.get("aria",""), fallback_year)
                    if not parsed:
                        continue
                    d, (h1,m1), (h2,m2) = parsed
                    title, room = smart_summary_and_room(b.get("txt",""))
                    items.append({
                        "date": d,
                        "start": (h1, m1),
                        "end": (h2, m2),
                        "title": title.strip() or "Cours",
                        "room": room.strip()
                    })
        except:
            pass
    return items

def merge_adjacent_same_course(slots: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """
    Fusionne sur la même journée les segments consécutifs/chevauchants
    (même titre + même salle).
    """
    slots = sorted(slots, key=lambda x: (x["date"], x["title"], x["room"], x["start"]))
    out = []
    for s in slots:
        if not out:
            out.append(s); continue
        last = out[-1]
        if (last["date"] == s["date"] and
            _norm(last["title"]) == _norm(s["title"]) and
            _norm(last["room"])  == _norm(s["room"])):
            # chevauchement ou joints (<=5 min de gap)
            l_end = last["end"][0]*60 + last["end"][1]
            s_sta = s["start"][0]*60 + s["start"][1]
            if s_sta - l_end <= 5:
                # étend la fin
                if (s["end"][0]*60 + s["end"][1]) > l_end:
                    last["end"] = s["end"]
                continue
        out.append(s)
    return out

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
        page.set_default_timeout(TIMEOUT_MS)

        login_ent(page)
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # Semaine X -> X+N
        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1

        # on lit l’année sur l’entête (pour construire les dates depuis 'aria-label')
        _, year_guess = read_week_header_and_year(pronote)

        for week_idx in range(start_idx, end_idx + 1):
            used_tab = goto_week_by_index(pronote, week_idx)
            pronote.wait_for_timeout(400)
            accept_cookies_any(pronote)

            # re-lire l’année si l’entête change (année scolaire chevauchante)
            header_txt, y = read_week_header_and_year(pronote)
            year = y or year_guess
            # extraction sûre : seulement les blocs cours
            slots = extract_courses_for_week(pronote, year)
            slots = merge_adjacent_same_course(slots)

            print(f"Semaine {week_idx}: {len(slots)} cours, header='{header_txt}'")

            now = datetime.now()
            for s in slots:
                start_dt = datetime(s["date"].year, s["date"].month, s["date"].day,
                                    s["start"][0], s["start"][1])
                end_dt   = datetime(s["date"].year, s["date"].month, s["date"].day,
                                    s["end"][0], s["end"][1])
                # filtre fenêtre raisonnable
                if end_dt < (now - timedelta(days=60)) or start_dt > (now + timedelta(days=120)):
                    continue

                title = f"{TITLE_PREFIX}{s['title']}"
                event_id = make_event_id(start_dt, end_dt, title, s["room"])

                body = {
                    "summary": title,
                    "location": s["room"],
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                }

                try:
                    act = upsert_event_by_id(svc, CALENDAR_ID, event_id, body)
                    created += (act == "created")
                    updated += (act == "updated")
                except HttpError as e:
                    print(f"[GCAL] {e}")

            # si onglet ko, on tente la flèche/semaine suivante
            if not used_tab and week_idx < end_idx:
                # bouton "Semaine suivante" (selon version)
                if not click_first_in_frames(pronote, [
                    'a:has-text("Semaine suivante")',
                    'button[title*="suivante"]',
                    'button[aria-label*="suivante"]'
                ]):
                    break
                pronote.wait_for_timeout(500)

        browser.close()

    print(f"Terminé. créés={created}, maj={updated}")

if __name__ == "__main__":
    try:
        run()
    except Exception as ex:
        try: os.makedirs(SCREEN_DIR, exist_ok=True)
        except: pass
        print(f"[FATAL] {ex}")
        sys.exit(1)
