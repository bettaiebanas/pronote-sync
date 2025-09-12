# -*- coding: utf-8 -*-
"""
pronote_playwright_to_family_mo.py

- Se connecte à l’ENT, ouvre PRONOTE
- Va sur Emploi du temps (via tes sélecteurs configurables)
- Parcourt n semaines (onglets j_1, j_2, … ou fallback “semaine suivante”)
- Extrait les séances (horaires, matières) depuis le frame EDT
- Fusionne les segments consécutifs identiques (EVITE la “même barre coupée en 2”)
- Upsert idempotent dans Google Calendar via une propriété privée (mo_hash)

Env (à placer dans ton workflow) :
  PRONOTE_USER, PRONOTE_PASS
  ENT_URL, PRONOTE_URL
  CALENDAR_ID
  HEADFUL=1 (pour voir le navigateur)
  TIMETABLE_PRE_SELECTOR   (ex: #GInterface\.Instances\[0\]\.Instances\[1\]_Combo5)
  TIMETABLE_SELECTOR       (ex: #GInterface\.Instances\[0\]\.Instances\[1\]_Liste_niveau5 > ul > li:nth-child(1) > div > div)
  TIMETABLE_FRAME          (ex: parent.html)
  WEEK_TAB_TEMPLATE        (ex: #GInterface\.Instances\[2\]\.Instances\[0\]_j_{n})
  FETCH_WEEKS_FROM=1
  WEEKS_TO_FETCH=4
  WAIT_AFTER_NAV_MS=1000
  CLICK_TOUT_VOIR=1
  DEBUG=1 (logs détaillés)
"""

import os, sys, re, time, json, hashlib, unicodedata
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =========================
# CONFIG (depuis ENV)
# =========================
ENT_URL       = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome").strip()
PRONOTE_URL   = os.getenv("PRONOTE_URL", "").strip()  # si vide: on clique la tuile PRONOTE
ENT_USER      = os.getenv("PRONOTE_USER", "").strip()
ENT_PASS      = os.getenv("PRONOTE_PASS", "").strip()

# Chemin manuel vers la page EDT (fourni par toi)
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()  # ex "parent.html"

# Les onglets semaine (tes j_1, j_2, j_3…)
WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()     # p.ex. "#GInterface\\.Instances\\[2\\]\\.Instances\\[0\\]_j_{n}"
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))

WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"
HEADFUL           = os.getenv("HEADFUL", "0") == "1"
DEBUG             = os.getenv("DEBUG", "0") == "1"

# Conteneurs EDT (ceux que tu m’as envoyés – on cible ces zones pour éviter la home)
# Tu peux chaîner plusieurs sélecteurs (séparés par des virgules)
TIMETABLE_CONTAINERS = os.getenv(
    "TIMETABLE_CONTAINERS",
    "#id_145_cont1, #id_145_cont0 > div:nth-child(2)"  # d’après tes captures
).strip()

# Google Calendar
CALENDAR_ID   = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com").strip()
TITLE_PREFIX  = os.getenv("TITLE_PREFIX", "[Mo] ").strip()
COLOR_ID      = os.getenv("COLOR_ID", "6").strip()  # 6 = orange
SCOPES        = ["https://www.googleapis.com/auth/calendar"]
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"

# Timeouts & captures
TIMEOUT_MS  = 120_000
SCREEN_DIR  = "screenshots"

# =========================
# LOG helpers
# =========================
def dbg(msg: str):
    if DEBUG:
        print(msg, flush=True)

def dump_frame_html(fr, tag: str):
    """Sauvegarde le HTML du frame pour inspection (artifact)."""
    try:
        os.makedirs(SCREEN_DIR, exist_ok=True)
        html = fr.content()
        path = os.path.join(SCREEN_DIR, f"frame-{tag}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        dbg(f"[DBG] HTML du frame dumpé -> {path} (len={len(html)})")
    except Exception as e:
        dbg(f"[DBG] dump_frame_html erreur: {e}")

# =========================
# Google Calendar
# =========================
def get_gcal_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            dbg(f"[DBG] Chargement token.json KO: {e}")
            creds = None
    if not creds or not creds.valid:
        from google.auth.transport.requests import Request
        if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds, cache_discovery=False)

def assert_calendar_exists(svc, cal_id: str):
    try:
        lst = svc.calendarList().list().execute()
        items = lst.get("items", [])
        dbg(f"[DBG] CalendarList loaded: {len(items)} calendars. CALENDAR_ID present? {any(c.get('id')==cal_id for c in items)}")
    except Exception as e:
        dbg(f"[DBG] CalendarList error: {e}")

# Dédup via propriété privée (pas de champ id custom côté insert)
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
        dbg(f"[GCAL-LIST] {e}")
        return None

def upsert_event_by_hash(svc, cal_id: str, h: str, body: Dict[str, Any]) -> str:
    ex = find_event_by_hash(svc, cal_id, h)
    try:
        if ex:
            ev_id = ex["id"]
            svc.events().update(calendarId=cal_id, eventId=ev_id, body=body, sendUpdates="none").execute()
            return "updated"
        else:
            svc.events().insert(calendarId=cal_id, body=body, sendUpdates="none").execute()
            return "created"
    except HttpError as e:
        dbg(f"[GCAL-UPSERT] {e}")
        raise

# =========================
# Parsing EDT
# =========================
HOUR_RE = re.compile(r'(?P<h>\d{1,2})\s*[:hH]\s*(?P<m>\d{2})', re.I)

def parse_timespan(text: str) -> Optional[Tuple[Tuple[int,int], Tuple[int,int]]]:
    times = HOUR_RE.findall(text or "")
    if len(times) >= 2:
        (h1,m1),(h2,m2) = times[0], times[1]
        return (int(h1),int(m1)), (int(h2),int(m2))
    return None

DOW_MAP = {
    "lun": 0, "mar": 1, "mer": 2, "jeu": 3, "ven": 4, "sam": 5, "dim": 6,
    "lundi":0,"mardi":1,"mercredi":2,"jeudi":3,"vendredi":4,"samedi":5,"dimanche":6
}

def guess_dayindex_from_text(text: str) -> Optional[int]:
    t = (text or "").lower()
    for key, idx in DOW_MAP.items():
        if key in t:
            return idx
    return None

def parse_aria_label(label: str) -> Dict[str, Any]:
    """Retourne {start:(h,m), end:(h,m), summary, room}"""
    d = {"start": None, "end": None, "summary": None, "room": ""}
    lab = " ".join((label or "").split())

    tspan = parse_timespan(lab)
    if tspan: d["start"], d["end"] = tspan

    m_room = re.search(r'(?:Salle|Salles?)\s*([A-Za-z0-9\-_. ]+)', lab, re.IGNORECASE)
    if m_room:
        d["room"] = m_room.group(1).strip()

    summary = lab
    # retirer "de 08h05 à 09h00" ou "08:05 - 09:00" en tête
    summary = re.sub(r'^\s*(?:de\s*)?\d{1,2}\s*[:hH]\s*\d{2}\s*(?:[–\-àa]\s*)?\d{1,2}\s*[:hH]\s*\d{2}\s*', '', summary, flags=re.I)
    summary = re.sub(r'(Salle|Salles?).*$', '', summary, flags=re.I)
    summary = re.sub(r'(Prof\.?:.*)$', '', summary, flags=re.I)
    summary = summary.strip(" -–")
    d["summary"] = summary if summary else "Cours"
    return d

def monday_of_week(text_header: str) -> Optional[datetime]:
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{4}).*?(\d{1,2}/\d{1,2}/\d{4})', text_header or "")
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y")
        except:
            return None
    return None

def to_datetime(base_monday: Optional[datetime], day_idx: Optional[int], hm: Tuple[int,int]) -> datetime:
    """Si lundi connu et day_idx (0..6) connu → lundi+day_idx."""
    if base_monday is not None and day_idx is not None and 0 <= int(day_idx) <= 6:
        base = base_monday + timedelta(days=int(day_idx))
    else:
        base = datetime.now()
    return base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

# =========================
# Playwright helpers
# =========================
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

def click_css_in_frames(page, css: str, frame_url_contains: str = "", screenshot_tag: str = "") -> bool:
    if not css:
        return False
    for fr in page.frames:
        try:
            if frame_url_contains and frame_url_contains not in (fr.url or ""):
                continue
        except:
            pass
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
            dbg(f"[NAV] click_css_in_frames fail in {getattr(fr, 'url', '?')}: {e}")
    return False

def accept_cookies_any(page):
    texts = [
        "Tout accepter","Accepter tout","J'accepte","Accepter",
        "OK","Continuer","J’ai compris","J'ai compris"
    ]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_in_frames(page, sels)

def get_timetable_frame(page):
    """Choisit le frame EDT et logue des stats pour débug."""
    frs = page.frames
    dbg(f"[DBG] Frames: {len(frs)}")
    for i, fr in enumerate(frs):
        url = ""
        try: url = fr.url
        except: pass
        try:
            stats = fr.evaluate("""
              (selStr) => {
                const out = { hasZone: false, containers: 0, rxCandidates: 0, bodyLen: 0 };
                try { out.bodyLen = (document.body.innerText || '').length; } catch {}
                try { out.hasZone = !!document.querySelector('.zoneSemaines'); } catch {}
                try {
                  const sels = (selStr || '').split(',').map(s => s.trim()).filter(Boolean);
                  for (const s of sels) out.containers += document.querySelectorAll(s).length;
                } catch {}
                try {
                  const rx = /\\b\\d{1,2}\\s*[hH:]\\s*\\d{2}.*\\b\\d{1,2}\\s*[hH:]\\s*\\d{2}\\b/;
                  const nodes = Array.from(document.querySelectorAll('[aria-label],[title],*'));
                  let c = 0;
                  for (const e of nodes) {
                    const lab = (e.getAttribute && (e.getAttribute('aria-label') || e.getAttribute('title'))) || (e.innerText||'').trim();
                    if (!lab || lab.length > 260) continue;
                    if (rx.test(lab)) c++;
                    if (c > 1500) break;
                  }
                  out.rxCandidates = c;
                } catch {}
                return out;
              }
            """, TIMETABLE_CONTAINERS)
        except Exception as e:
            stats = {"hasZone": False, "containers": 0, "rxCandidates": 0, "bodyLen": 0}
            dbg(f"[DBG]  frame[{i}] stats error: {e}")
        dbg(f"      - [{i}] {url} | hasZone={stats['hasZone']} containers={stats['containers']} rxCandidates~{stats['rxCandidates']} bodyLen={stats['bodyLen']}")

    chosen = None
    pref = TIMETABLE_FRAME.lower().strip() if TIMETABLE_FRAME else ""
    if pref:
        for fr in frs:
            try:
                if pref in (fr.url or "").lower():
                    chosen = fr
                    break
            except:
                pass

    if not chosen:
        best_score = -1
        best_fr = None
        for fr in frs:
            try:
                st = fr.evaluate("""
                  (selStr) => {
                    const out = { hasZone: false, containers: 0, rx: 0 };
                    try { out.hasZone = !!document.querySelector('.zoneSemaines'); } catch {}
                    try {
                      const sels = (selStr || '').split(',').map(s => s.trim()).filter(Boolean);
                      for (const s of sels) out.containers += document.querySelectorAll(s).length;
                    } catch {}
                    try {
                      const rx = /\\b\\d{1,2}\\s*[hH:]\\s*\\d{2}.*\\b\\d{1,2}\\s*[hH:]\\s*\\d{2}\\b/;
                      const nodes = Array.from(document.querySelectorAll('[aria-label],[title],*'));
                      let c = 0;
                      for (const e of nodes) {
                        const lab = (e.getAttribute && (e.getAttribute('aria-label') || e.getAttribute('title'))) || (e.innerText||'').trim();
                        if (!lab || lab.length > 260) continue;
                        if (rx.test(lab)) c++;
                        if (c > 2000) break;
                      }
                      out.rx = c;
                    } catch {}
                    return out;
                  }
                """, TIMETABLE_CONTAINERS)
                score = (5 if st["hasZone"] else 0) + min(st["containers"], 5) + min(st["rx"]//25, 5)
                if score > best_score:
                    best_score = score
                    best_fr = fr
            except:
                pass
        chosen = best_fr

    if not chosen:
        for fr in frs:
            try:
                if fr.evaluate("() => /Emploi du temps/i.test((document.body.innerText||''))"):
                    chosen = fr
                    break
            except:
                pass

    if not chosen:
        raise TimeoutError("Aucun frame EDT trouvé")

    dbg(f"[DBG] Timetable frame choisi: {getattr(chosen, 'url', '?')}")
    dump_frame_html(chosen, "chosen")
    return chosen

# =========================
# Navigation
# =========================
def login_ent(page):
    os.makedirs(SCREEN_DIR, exist_ok=True)
    page.set_default_timeout(TIMEOUT_MS)
    dbg(f"[CFG] ENT_URL={ENT_URL} | PRONOTE_URL={PRONOTE_URL}")
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

    # Ton chemin perso : Vie scolaire -> Emploi du temps
    if TIMETABLE_PRE_SELECTOR:
        click_css_in_frames(pronote_page, TIMETABLE_PRE_SELECTOR, TIMETABLE_FRAME, "pre-selector")

    if TIMETABLE_SELECTOR:
        if click_css_in_frames(pronote_page, TIMETABLE_SELECTOR, TIMETABLE_FRAME, "timetable-selector"):
            accept_cookies_any(pronote_page)
            try:
                get_timetable_frame(pronote_page)
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-custom-selector.png", full_page=True)
                return
            except Exception:
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-custom-timeout.png", full_page=True)

    # Fallback heuristiques (liens/texte)
    attempts = [
        ["Emploi du temps", "Mon emploi du temps", "Emplois du temps"],
        ["Planning", "Agenda"],
        ["Vie scolaire", "Emploi du temps"],
    ]
    for i, pats in enumerate(attempts, 1):
        for pat in pats:
            if click_first_in_frames(pronote_page, [
                f'role=link[name="{pat}"]',
                f'role=button[name="{pat}"]',
                f'a:has-text("{pat}")','button:has-text("{pat}")'
            ]):
                accept_cookies_any(pronote_page)
                try:
                    get_timetable_frame(pronote_page)
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-ready-{i}-{pat}.png", full_page=True)
                    return
                except Exception:
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-not-ready-{i}-{pat}.png", full_page=True)
        pronote_page.wait_for_timeout(600)

    # Dernier essai : si ça a fini par charger
    try:
        get_timetable_frame(pronote_page)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-ready-fallback.png", full_page=True)
        return
    except Exception:
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-NOT-found.png", full_page=True)
        raise RuntimeError("Impossible d’atteindre l’Emploi du temps.")

def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        click_first_in_frames(page, [
            'button:has-text("Tout voir")','button:has-text("Voir tout")','button:has-text("Tout afficher")',
            'a:has-text("Tout voir")','a:has-text("Voir tout")','a:has-text("Tout afficher")'
        ])
        page.wait_for_timeout(400)

def goto_week_by_index(page, n: int) -> bool:
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)
    ok = click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}")
    if ok:
        try:
            get_timetable_frame(page)  # revalide
        except Exception:
            pass
    return ok

def iter_next_week(page) -> bool:
    if click_first_in_frames(page, [
        'button[title*="suivante"]','button[aria-label*="suivante"]','button:has-text("→")',
        'a[title*="suivante"]','a:has-text("Semaine suivante")'
    ]):
        accept_cookies_any(page)
        try:
            get_timetable_frame(page)
        except Exception:
            pass
        page.screenshot(path=f"{SCREEN_DIR}/09-pronote-next-week.png", full_page=True)
        return True
    return False

# =========================
# Extraction depuis le frame EDT
# =========================
def extract_week_info(page) -> Dict[str, Any]:
    fr = get_timetable_frame(page)
    return extract_week_info_from_frame(fr)

def extract_week_info_from_frame(fr) -> Dict[str, Any]:
    containers = [c.strip() for c in (TIMETABLE_CONTAINERS or "").split(",") if c.strip()]
    dbg(f"[DBG]   containers ciblés: {containers}")

    # Header
    header = ""
    try:
        header = fr.evaluate("""
          (selectors) => {
            const pick = (arr, fn) => { for (const sel of arr) { try { const v = fn(sel); if (v) return v; } catch{} } return ''; };
            const h = pick(selectors, sel => {
              const n = document.querySelector(sel);
              return (n && n.innerText || '').trim();
            }) || (document.querySelector('.zoneSemaines')||{}).innerText || '';
            return (h||'').trim();
          }
        """, containers) or ""
    except:
        header = ""

    # Compter les conteneurs
    try:
        cnts = fr.evaluate("(selectors)=>selectors.flatMap(s=>Array.from(document.querySelectorAll(s))).length", containers)
        dbg(f"[DBG]   nb noeuds matchant containers: {cnts}")
    except:
        pass

    # Repérer tuiles avec heuristique horaires + largeur > 5px, et bucketer en colonnes (jour)
    data = fr.evaluate("""
      (selectors) => {
        const roots = [];
        for (const sel of selectors) document.querySelectorAll(sel).forEach(n => roots.push(n));
        if (roots.length === 0) roots.push(document.body);

        const rx = /\\b\\d{1,2}\\s*[hH:]\\s*\\d{2}.*\\b\\d{1,2}\\s*[hH:]\\s*\\d{2}\\b/;
        const tilesRaw = [];
        const previews = [];
        const seen = new Set();

        for (const root of roots) {
          const nodes = Array.from(root.querySelectorAll('[aria-label],[title],*'));
          for (const e of nodes) {
            let lab = (e.getAttribute && (e.getAttribute('aria-label') || e.getAttribute('title'))) || (e.innerText||'').trim();
            if (!lab) continue;
            if (lab.length > 260) continue;
            if (!rx.test(lab)) continue;
            const r = e.getBoundingClientRect();
            if (!r || r.width < 5 || r.height < 5) continue;

            const key = lab + '|' + Math.round(r.left/10);
            if (seen.has(key)) continue;
            seen.add(key);

            tilesRaw.push({ label: lab, cx: r.left + r.width/2 });
            if (previews.length < 5) previews.push(lab);
          }
        }

        // Buckets horizontaux -> dayIndex
        tilesRaw.sort((a,b)=>a.cx-b.cx);
        const tiles = [];
        if (tilesRaw.length > 0) {
          const TH = Math.max(30, (tilesRaw[tilesRaw.length-1].cx - tilesRaw[0].cx)/30);
          let cur = [tilesRaw[0]], buckets = [];
          for (let i=1;i<tilesRaw.length;i++) {
            if (Math.abs(tilesRaw[i].cx - tilesRaw[i-1].cx) <= TH) cur.push(tilesRaw[i]);
            else { buckets.push(cur); cur = [tilesRaw[i]]; }
          }
          buckets.push(cur);
          for (let d=0; d<buckets.length; d++) {
            for (const t of buckets[d]) tiles.push({ label: t.label, dayIndex: Math.min(d, 6) });
          }
        }
        const bodyTxt = (document.body.innerText||'');
        const hdr = bodyTxt.includes('Emploi du temps') ? bodyTxt : '';
        return { header: hdr, tiles, previews };
      }
    """, containers) or {}

    tiles = data.get("tiles") or []
    previews = data.get("previews") or []
    if not header:
        header = data.get("header") or ""
    dbg(f"[DBG]   frame header len={len(header)} tiles={len(tiles)}")
    if previews:
        dbg("[DBG]   5 premières tuiles:")
        for p in previews:
            dbg(f"        - {p[:200]}")

    return {"header": header, "tiles": tiles, "monday": monday_of_week(header)}

# =========================
# Post-traitements des cours
# =========================
def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_entries_from_tiles(tiles: List[Dict[str,Any]], header: str, d0: Optional[datetime]) -> List[Dict[str,Any]]:
    entries = []
    for t in tiles:
        lab = (t.get("label") or "").strip()
        if not lab:
            continue
        parsed = parse_aria_label(lab)
        if not parsed["start"] or not parsed["end"]:
            continue

        # dayIndex : d'abord ce qu’on a bucketé, sinon essaie par texte (lun., mar., …)
        di = t.get("dayIndex")
        if di is None:
            di = guess_dayindex_from_text(lab)

        start_dt = to_datetime(d0, di, parsed["start"])
        end_dt   = to_datetime(d0, di, parsed["end"])

        subj = normalize_text(parsed["summary"] or "Cours")
        room = normalize_text(parsed["room"] or "")

        entries.append({
            "start": start_dt,
            "end":   end_dt,
            "title": subj,
            "room":  room,
        })
    return entries

def merge_adjacent(entries: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """Fusionne les segments consécutifs (même jour, même titre, même salle, end==start du suivant)."""
    if not entries:
        return []
    # trie par (date, start)
    entries = sorted(entries, key=lambda e: (e["start"].date(), e["start"], e["end"], e["title"], e["room"]))
    out = [entries[0]]
    for e in entries[1:]:
        last = out[-1]
        if (last["title"] == e["title"] and
            last["room"]  == e["room"]  and
            last["start"].date() == e["start"].date() and
            last["end"] == e["start"]):
            # fusion
            last["end"] = e["end"]
        else:
            out.append(e)
    return out

# =========================
# MAIN
# =========================
def run():
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("Identifiants ENT manquants: PRONOTE_USER / PRONOTE_PASS.")

    svc = get_gcal_service()
    assert_calendar_exists(svc, CALENDAR_ID)

    created = updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = context.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        # ENT → PRONOTE → EDT
        login_ent(page)
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # réglages d’itération
        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1
        dbg(f"[CFG] Weeks: {start_idx}..{end_idx}")

        for week_idx in range(start_idx, end_idx + 1):
            used_tab = goto_week_by_index(pronote, week_idx)
            accept_cookies_any(pronote)
            ensure_all_visible(pronote)

            info  = extract_week_info(pronote)
            d0    = info.get("monday")
            tiles = info.get("tiles") or []
            hdr   = (info.get("header") or "").replace("\n", " ")[:160]

            # debug & capture
            try:
                pronote.screenshot(path=f"{SCREEN_DIR}/10-week-{week_idx}.png", full_page=True)
            except:
                pass
            print(f"Semaine {week_idx}: {len(tiles)} cours, header='{hdr}'")

            # Construire, fusionner, dédupliquer
            entries = build_entries_from_tiles(tiles, hdr, d0)
            dbg(f"[DBG]   entries construits: {len(entries)}")
            entries = merge_adjacent(entries)
            # petite dédup brutale par (start,end,title,room) si doublons résiduels
            seen = set()
            unique = []
            for e in entries:
                key = (e["start"], e["end"], e["title"], e["room"])
                if key not in seen:
                    seen.add(key)
                    unique.append(e)
            entries = unique
            dbg(f"[DBG]   après fusion/dedup: {len(entries)}")

            # Filtre de sécurité (fenêtre +-90j)
            now = datetime.now()
            filtered = []
            for e in entries:
                if e["end"] < (now - timedelta(days=21)):
                    continue
                if e["start"] > (now + timedelta(days=120)):
                    continue
                filtered.append(e)
            entries = filtered

            # Upsert GCal
            for e in entries:
                title = f"{TITLE_PREFIX}{e['title']}".strip()
                hash_id = make_hash_id(e["start"], e["end"], title, e["room"])
                body = {
                    "summary": title,
                    "location": e["room"],
                    "start": {"dateTime": e["start"].isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": e["end"].isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"mo_hash": hash_id, "source": "pronote_playwright"}},
                }
                try:
                    act = upsert_event_by_hash(svc, CALENDAR_ID, hash_id, body)
                    if act == "created": created += 1
                    else: updated += 1
                except HttpError:
                    # déjà loggé dans upsert_event_by_hash
                    pass

            # si on n’a pas réussi à cliquer l’onglet j_n, tente “semaine suivante”
            if not used_tab and week_idx < end_idx:
                if not iter_next_week(pronote):
                    break

        browser.close()

    print(f"Terminé. créés={created}, maj={updated}")

if __name__ == "__main__":
    try:
        os.makedirs(SCREEN_DIR, exist_ok=True)
    except:
        pass
    try:
        run()
    except Exception as ex:
        try:
            os.makedirs(SCREEN_DIR, exist_ok=True)
        except:
            pass
        print(f"[FATAL] {ex}")
        sys.exit(1)
