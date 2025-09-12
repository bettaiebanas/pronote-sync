# -*- coding: utf-8 -*-
import os, sys, re, hashlib, unicodedata, time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG =========
ENT_URL     = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL = os.getenv("PRONOTE_URL", "")  # si vide : on clique la tuile PRONOTE depuis l’ENT
ENT_USER    = os.getenv("PRONOTE_USER", "")
ENT_PASS    = os.getenv("PRONOTE_PASS", "")

# Sélecteurs validés avec toi (ENT → Vie scolaire → Emploi du temps)
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()   # Vie scolaire
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()       # Emploi du temps
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()          # ex: parent.html
WEEK_TAB_TEMPLATE      = os.getenv("WEEK_TAB_TEMPLATE", "").strip()        # ex: #GInterface\.Instances\[2\]\.Instances\[0\]_j_{n}

FETCH_WEEKS_FROM = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH   = int(os.getenv("WEEKS_TO_FETCH",   "4"))
WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

CALENDAR_ID  = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX = "[Mo] "
COLOR_ID     = "6"   # orange
HEADFUL      = os.getenv("HEADFUL", "0") == "1"

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts (ms)
TIMEOUT_MS = 120_000
SCREEN_DIR = "screenshots"

# ========= Google Calendar =========
def gcal_service():
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

# Dédup via propriété étendue privée
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
        ev_id = existing["id"]
        svc.events().update(calendarId=cal_id, eventId=ev_id, body=body, sendUpdates="none").execute()
        return "updated"
    else:
        svc.events().insert(calendarId=cal_id, body=body, sendUpdates="none").execute()
        return "created"

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

def accept_cookies_any(page):
    texts = [
        "Tout accepter","Accepter tout","J'accepte","Accepter",
        "OK","Continuer","J’ai compris","J'ai compris"
    ]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_in_frames(page, sels)

def _frame_has_timetable_js():
    return r"""
      () => {
        const txt = (document.body.innerText || '').replace(/\s+/g,' ');
        const hasTitle = /Emploi du temps/i.test(txt) || /Planning|Agenda/i.test(txt);
        const hasWeek  = /(Semaine|du\s+\d{1,2}\/\d{1,2}\/\d{4}\s+au\s+\d{1,2}\/\d{1,2}\/\d{4})/i.test(txt);
        const hasTimes = /\d{1,2}\s*[h:]\s*\d{2}/i.test(txt);
        return (hasTitle && (hasTimes || hasWeek)) || (hasWeek && hasTimes);
      }
    """

def wait_timetable_any_frame(page, timeout_ms=120_000):
    deadline = time.time() + timeout_ms/1000.0
    js = _frame_has_timetable_js()
    while time.time() < deadline:
        for fr in page.frames:
            try:
                if fr.evaluate(js):
                    return fr
            except:
                pass
        page.wait_for_timeout(500)
    raise TimeoutError("Timetable not found in any frame")

def wait_for_timetable_ready(page, timeout_ms=TIMEOUT_MS):
    return wait_timetable_any_frame(page, timeout_ms=timeout_ms)

def click_text_anywhere(page, patterns: List[str]) -> bool:
    for frame in page.frames:
        for pat in patterns:
            for loc in [
                frame.get_by_role("link", name=re.compile(pat, re.I)),
                frame.get_by_role("button", name=re.compile(pat, re.I)),
            ]:
                try:
                    if loc.count() > 0:
                        loc.first.click()
                        return True
                except:
                    pass
            try:
                found = frame.evaluate(
                    r"""
                    (pat) => {
                      const rx = new RegExp(pat, 'i');
                      const nodes = Array.from(document.querySelectorAll('body *')).filter(
                        e => (e.innerText || '').match(rx)
                      );
                      for (const n of nodes) {
                        let p = n;
                        while (p) {
                          if (p.tagName === 'A' || p.tagName === 'BUTTON' || p.getAttribute('role') === 'button' || p.onclick) {
                            p.click();
                            return true;
                          }
                          p = p.parentElement;
                        }
                      }
                      return false;
                    }
                    """,
                    pat
                )
                if found:
                    return True
            except:
                pass
    return False

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
                page.wait_for_timeout(int(os.getenv("WAIT_AFTER_NAV_MS","800")))
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

    # Chemin personnalisé "Vie scolaire" -> "Emploi du temps"
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

    # Heuristique
    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=15_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-ready.png", full_page=True)
        return fr
    except TimeoutError:
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-NOT-found.png", full_page=True)
        raise RuntimeError("Impossible d’atteindre l’Emploi du temps.")

def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        click_text_anywhere(page, ["Tout voir", "Voir tout", "Tout afficher"])
        page.wait_for_timeout(400)

def goto_week_by_index(page, n: int) -> bool:
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)
    ok = click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}")
    if ok:
        try:
            wait_timetable_any_frame(page, timeout_ms=8_000)
        except TimeoutError:
            pass
    return ok

def extract_week_info_from_target(target) -> Dict[str, Any]:
    # 'target' is a Frame (preferred) or Page
    header_text = ""
    try:
        header_text = target.evaluate("""() => (document.body.innerText || '').substring(0, 1500)""")
    except:
        header_text = ""

    d0 = monday_of_week(header_text)

    tiles = target.evaluate(r"""
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
      const rx = /\d{1,2}[:hH]\d{2}.*\d{1,2}[:hH]\d{2}/;
      document.querySelectorAll('[aria-label]').forEach(e => {
        const v = e.getAttribute('aria-label'); if (v && rx.test(v)) add(e, v);
      });
      document.querySelectorAll('[title]').forEach(e => {
        const v = e.getAttribute('title'); if (v && rx.test(v)) add(e, v);
      });
      document.querySelectorAll('*').forEach(e => {
        const t = (e.innerText || '').trim();
        if (t && rx.test(t) && t.length < 200) add(e, t);
      });
      return out;
    }
    """)
    return {"monday": d0, "tiles": tiles, "header": header_text}

# ========= Main =========
def run():
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("Identifiants ENT manquants: PRONOTE_USER / PRONOTE_PASS.")

    svc = gcal_service()
    created = updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = context.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        login_ent(page)
        pronote = open_pronote(context, page)
        target = goto_timetable(pronote)  # Frame cible, sinon page

        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1
        print(f"[CFG] Weeks: {start_idx}..{end_idx}")

        for week_idx in range(start_idx, end_idx + 1):
            used_tab = goto_week_by_index(pronote, week_idx)
            accept_cookies_any(pronote)
            ensure_all_visible(pronote)

            # Re-detect target after navigation
            try:
                target = wait_timetable_any_frame(pronote, timeout_ms=10_000)
            except:
                target = pronote

            info  = extract_week_info_from_target(target)
            d0    = info["monday"]
            tiles = info["tiles"] or []
            hdr   = (info.get("header") or "").replace("\n", " ")[:200]
            print(f"Semaine {week_idx}: {len(tiles)} cours, header='{hdr}'")

            # Construire événements
            entries = []
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

                title = f"{TITLE_PREFIX}{(parsed['summary'] or 'Cours').strip()}"
                room  = parsed["room"]

                entries.append((start_dt, end_dt, title, room))

            print(f"[DBG]   entries construits: {len(entries)}")

            # Dédup pures (même tranche exacte) - côté script
            by_key = {}
            for (s,e,tit,rm) in entries:
                k = (s.isoformat(), e.isoformat(), _norm(tit), _norm(rm))
                by_key[k] = (s,e,tit,rm)
            merged = list(by_key.values())
            print(f"[DBG]   après dédup: {len(merged)}")

            # Upsert côté Google
            for (start_dt, end_dt, title, room) in merged:
                h = make_hash_id(start_dt, end_dt, title, room)
                body = {
                    "summary": title,
                    "location": room,
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"mo_hash": h, "source": "pronote_playwright"}},
                }
                try:
                    action = upsert_event_by_hash(svc, CALENDAR_ID, h, body)
                    if action == "created": created += 1
                    else: updated += 1
                except HttpError as e:
                    print(f"[GCAL] {e}")

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



if __name__=="__main__":
    run()
