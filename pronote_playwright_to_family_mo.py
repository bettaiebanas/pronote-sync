# pronote_playwright_to_family_mo.py
import os, sys, re, hashlib, unicodedata, time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG (identiques) =========
ENT_URL       = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL   = os.getenv("PRONOTE_URL", "")
ENT_USER      = os.getenv("PRONOTE_USER", "")
ENT_PASS      = os.getenv("PRONOTE_PASS", "")

TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()

WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))

WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

CALENDAR_ID   = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX  = "[Mo] "
COLOR_ID      = "6"
HEADFUL       = os.getenv("HEADFUL", "0") == "1"

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

TIMEOUT_MS  = 120_000
SCREEN_DIR  = "screenshots"

# ========= Google Calendar =========
def get_gcal_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        from google.auth.transport.requests import Request
        if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

# ---- id stable (dé-dup blindée) ----
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def make_hash_id(start: datetime, end: datetime, title: str, location: str) -> str:
    base = f"{start.isoformat()}|{end.isoformat()}|{_norm(title)}|{_norm(location)}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def find_event_by_hash(svc, cal_id: str, h: str):
    resp = svc.events().list(
        calendarId=cal_id,
        privateExtendedProperty=f"mo_hash={h}",
        singleEvents=True,
        maxResults=1,
        orderBy="startTime",
    ).execute()
    items = resp.get("items", [])
    return items[0] if items else None

def upsert_event_by_hash(svc, cal_id: str, h: str, body: Dict[str, Any]) -> str:
    existing = find_event_by_hash(svc, cal_id, h)
    if existing:
        svc.events().update(calendarId=cal_id, eventId=existing["id"], body=body, sendUpdates="none").execute()
        return "updated"
    svc.events().insert(calendarId=cal_id, body=body, sendUpdates="none").execute()
    return "created"

# ========= Parsing helpers =========
# 08:05, 8h05, 8 heures 05
TIME_UNIT = r'(?:h|:|heures?)'
HOUR_PAIR_RE = re.compile(rf'(\d{{1,2}})\s*{TIME_UNIT}\s*(\d{{2}}).*?(\d{{1,2}})\s*{TIME_UNIT}\s*(\d{{2}})', re.I)

MONTHS = {
    'janvier':1,'février':2,'fevrier':2,'mars':3,'avril':4,'mai':5,'juin':6,
    'juillet':7,'août':8,'aout':8,'septembre':9,'octobre':10,'novembre':11,'décembre':12,'decembre':12
}

def parse_times_from_text(text: str):
    m = HOUR_PAIR_RE.search(text or "")
    if not m: return None
    h1,m1,h2,m2 = m.groups()
    return (int(h1), int(m1)), (int(h2), int(m2))

def parse_dayindex_from_label(label: str, monday: Optional[datetime]) -> Optional[int]:
    if not monday: return None
    m = re.search(r'(?:du|le)\s+(\d{1,2})\s+([A-Za-zéûôà]+)', label or "", re.I)
    if not m: return None
    d = int(m.group(1))
    mon = MONTHS.get(unicodedata.normalize("NFKD", m.group(2).lower()).encode("ascii","ignore").decode(), None)
    if not mon: return None
    year = monday.year
    try:
        dt = datetime(year, mon, d)
    except ValueError:
        return None
    return (dt - monday).days if 0 <= (dt - monday).days <= 6 else None

def clean_summary_text(s: str) -> str:
    s = " ".join((s or "").split())
    # on raccourcit un peu les balises fréquentes
    s = re.sub(r'\b\[.*?\]\s*', '', s)  # supprime [4GPI], [LCE…], etc.
    return s.strip()

def monday_of_header(text_header: str) -> Optional[datetime]:
    m = re.search(r'(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})', text_header or "")
    if not m: return None
    return datetime.strptime(m.group(1), "%d/%m/%Y")

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

def accept_cookies_any(page):
    texts = ["Tout accepter","Accepter tout","J'accepte","Accepter","OK","Continuer","J’ai compris","J'ai compris"]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_in_frames(page, sels)

def wait_timetable_any_frame(page, timeout_ms=120_000):
    deadline = time.time() + timeout_ms/1000.0
    js = r"""
    () => {
      const txt = (document.body.innerText || '').replace(/\s+/g,' ');
      const hasTitle = /Emploi du temps/i.test(txt) || /Planning|Agenda/i.test(txt);
      const hasWeek  = /(Semaine|du\s+\d{1,2}\/\d{1,2}\/\d{4}\s+au\s+\d{1,2}\/\d{1,2}\/\d{4})/i.test(txt);
      const hasTimes = /\d{1,2}\s*(?:h|:|heures?)\s*\d{2}/i.test(txt);
      return (hasTitle && (hasTimes || hasWeek)) || (hasWeek && hasTimes);
    }"""
    while time.time() < deadline:
        for fr in page.frames:
            try:
                if fr.evaluate(js):
                    return fr
            except:
                pass
        page.wait_for_timeout(400)
    raise TimeoutError("Timetable not found in any frame")

def click_text_anywhere(page, patterns: List[str]) -> bool:
    for frame in page.frames:
        for pat in patterns:
            for loc in [frame.get_by_role("link", name=re.compile(pat,re.I)),
                        frame.get_by_role("button", name=re.compile(pat,re.I))]:
                try:
                    if loc.count() > 0:
                        loc.first.click(); return True
                except: pass
            try:
                found = frame.evaluate(r"""
                  (pat) => {
                    const rx = new RegExp(pat,'i');
                    const nodes = Array.from(document.querySelectorAll('body *'))
                      .filter(e => (e.innerText||'').match(rx));
                    for (const n of nodes){
                      let p=n;
                      while(p){
                        if (p.tagName==='A' || p.tagName==='BUTTON' || p.getAttribute('role')==='button' || p.onclick){ p.click(); return true; }
                        p = p.parentElement;
                      }
                    }
                    return false;
                  }""", pat)
                if found: return True
            except: pass
    return False

def click_css_in_frames(page, css: str, frame_url_contains: str = "", screenshot_tag: str = "") -> bool:
    if not css: return False
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
            raise RuntimeError("Tuile/lien PRONOTE introuvable après login ENT.")
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

    if TIMETABLE_PRE_SELECTOR:
        click_css_in_frames(pronote_page, TIMETABLE_PRE_SELECTOR, TIMETABLE_FRAME, "pre-selector")
    if TIMETABLE_SELECTOR:
        if click_css_in_frames(pronote_page, TIMETABLE_SELECTOR, TIMETABLE_FRAME, "timetable-selector"):
            accept_cookies_any(pronote_page)
            try:
                fr2 = wait_timetable_any_frame(pronote_page, timeout_ms=30_000)
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-custom-selector.png", full_page=True)
                return fr2
            except TimeoutError:
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-custom-timeout.png", full_page=True)

    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=10_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-already-here.png", full_page=True)
        return fr
    except TimeoutError:
        pass

    for i, pats in enumerate([["Emploi du temps","Mon emploi du temps","Emplois du temps"],
                              ["Planning","Agenda"],
                              ["Vie scolaire","Emploi du temps"]], 1):
        for pat in pats:
            if click_text_anywhere(pronote_page, [pat]):
                accept_cookies_any(pronote_page)
                try:
                    fr = wait_timetable_any_frame(pronote_page, timeout_ms=30_000)
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-ready-{i}-{pat}.png", full_page=True)
                    return fr
                except TimeoutError:
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-not-ready-{i}-{pat}.png", full_page=True)
        pronote_page.wait_for_timeout(600)

    raise RuntimeError("Impossible d’atteindre l’Emploi du temps (après multiples essais).")

def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        click_text_anywhere(page, ["Tout voir","Voir tout","Tout afficher"])
        page.wait_for_timeout(300)

def goto_week_by_index(page, n: int) -> bool:
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)
    ok = click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}")
    if ok:
        try: wait_timetable_any_frame(page, timeout_ms=20_000)
        except TimeoutError: pass
    return ok

# ========= Extraction =========
MONTHS_FR = {
    "janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"août":8,"aout":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,"decembre":12
}

def parse_aria_course(aria: str, header: str):
    """
    aria: 'Cours du 8 septembre de 9 heures 05 à 10 heures 00'
    header: 'du 08/09/2025 au 12/09/2025'  -> on récupère l'année
    """
    y = None
    m = re.search(r'du\s+(\d{2})/(\d{2})/(\d{4})', header)
    if m:
        y = int(m.group(3))
    rx = re.search(r'Cours du\s+(\d{1,2})\s+([a-zéûôîàèù]+)\s+de\s+(\d{1,2})\D?(\d{2})\s+à\s+(\d{1,2})\D?(\d{2})', aria, re.I)
    if not rx:
        return None, None
    d, mois = int(rx.group(1)), rx.group(2).lower()
    sh, sm, eh, em = map(int, (rx.group(3), rx.group(4), rx.group(5), rx.group(6)))
    mm = MONTHS_FR.get(mois)
    if not (y and mm):
        return None, None
    start = datetime(y, mm, d, sh, sm)
    end   = datetime(y, mm, d, eh, em)
    return start, end

def guess_room(text_block: str) -> str:
    lines = [l.strip() for l in (text_block or "").splitlines() if l.strip()]
    # souvent la salle est la dernière ligne (ex: '004' ou 'S11', 'L12')
    for l in reversed(lines):
        if re.search(r'\b([A-Z]{1,3}\d{1,3}|0\d{2}|S\d{1,2}|L\d{1,2})\b', l):
            return l
    return ""



def extract_week_info(pronote_page) -> Dict[str, Any]:
    # on s’assure qu’on est bien sur la frame EDT
    fr = wait_timetable_any_frame(pronote_page, timeout_ms=30_000)

    data = fr.evaluate(r"""
    () => {
      const res = { header:"", items:[] };

      const headerMatch = (document.body.innerText||"")
        .match(/du\s+\d{2}\/\d{2}\/\d{4}\s+au\s+\d{2}\/\d{2}\/\d{4}/i);
      res.header = headerMatch ? headerMatch[0] : "";

      document.querySelectorAll('div.cours-simple[aria-label^="Cours"]').forEach(c => {
        const aria = c.getAttribute('aria-label') || '';
        const tcell = c.querySelector('td[id$="_cont0"]');
        const text  = tcell ? tcell.innerText : (c.innerText || "");
        res.items.push({ aria, text });
      });
      return res;
    }
    """)

    return data

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

        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1

        for week_idx in range(start_idx, end_idx + 1):
            used_tab = goto_week_by_index(pronote, week_idx)
            accept_cookies_any(pronote)
            ensure_all_visible(pronote)

           info = extract_week_info(pronote)
hdr  = info.get("header","")
items = info.get("items", [])
print(f"Semaine {w+1}: {len(items)} cases, header='{hdr}'")

for it in items:
    start_dt, end_dt = parse_aria_course(it.get("aria",""), hdr)
    if not start_dt or not end_dt:
        continue

    # bornes (sécurité)
    now = datetime.now()
    if end_dt < (now - timedelta(days=21)) or start_dt > (now + timedelta(days=90)):
        continue

    block_text = it.get("text","").strip()
    summary = (block_text.splitlines()[0].strip() if block_text else "Cours")
    room    = guess_room(block_text)

    title = f"{TITLE_PREFIX}{summary}"
    hash_id = make_hash_id(start_dt, end_dt, title, room)  # ta dédup existante

    event = {
        "summary": title,
        "location": room,
        "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
        "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
        "colorId": COLOR_ID,
        "extendedProperties": {"private": {"mo_hash": hash_id, "source": "pronote_playwright"}},
    }

    try:
        action = upsert_event_by_hash(svc, CALENDAR_ID, hash_id, event)
        if action == "created": created += 1
        else: updated += 1
    except HttpError as e:
        print(f"[GCAL] {e}")


if __name__ == "__main__":
    try:
        os.makedirs(SCREEN_DIR, exist_ok=True)
        run()
    except Exception as ex:
        try: os.makedirs(SCREEN_DIR, exist_ok=True)
        except: pass
        print(f"[FATAL] {ex}")
        sys.exit(1)
