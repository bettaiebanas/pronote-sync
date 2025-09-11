# -*- coding: utf-8 -*-
# pronote_playwright_to_family_mo.py

import os, sys, re, time, hashlib, unicodedata
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG =========
ENT_URL     = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL = os.getenv("PRONOTE_URL", "")  # si vide, on clique la tuile PRONOTE
ENT_USER    = os.getenv("PRONOTE_USER", "")
ENT_PASS    = os.getenv("PRONOTE_PASS", "")

# Sélecteurs personnalisables (si besoin)
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()

WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()  # ex: #GInterface\.Instances\[2\]\.Instances\[0\]_j_{n}
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))

WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

# Google Calendar
CALENDAR_ID  = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX = "[Mo] "
COLOR_ID     = "6"  # orange

HEADFUL = os.getenv("HEADFUL", "0") == "1"

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts / captures
TIMEOUT_MS = 120_000
SCREEN_DIR = "screenshots"

# ========= Google Calendar =========
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
    try:
        svc = build("calendar","v3",credentials=creds)
        # petit log utile
        cal_list = svc.calendarList().list().execute().get("items", [])
        print(f"[DBG] CalendarList loaded: {len(cal_list)} calendars. CALENDAR_ID present? {any(c.get('id')==CALENDAR_ID for c in cal_list)}")
        return svc
    except Exception as e:
        print(f"[Google API] {e}")
        raise

def make_hash_id(start: datetime, end: datetime, title: str, location: str) -> str:
    base = f"{start.isoformat()}|{end.isoformat()}|{title}|{location}"
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

# ========= util dates (fr) =========
_MONTHS_FR = {
    "janvier":"01","janv":"01","jan":"01",
    "février":"02","fevrier":"02","févr":"02","fevr":"02","fév":"02","fev":"02",
    "mars":"03",
    "avril":"04","avr":"04",
    "mai":"05",
    "juin":"06",
    "juillet":"07","juil":"07",
    "août":"08","aout":"08",
    "septembre":"09","sept":"09","sep":"09",
    "octobre":"10","oct":"10",
    "novembre":"11","nov":"11",
    "décembre":"12","decembre":"12","déc":"12","dec":"12",
}

def _strip_accents(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode()

def _month_fr_to_num(s: str) -> str:
    k = _strip_accents(s.lower().replace('.','').strip())
    return _MONTHS_FR.get(k, "")

def _parse_year_from_header(txt: str) -> Optional[int]:
    m = re.search(r'\bdu\s+(\d{2})/(\d{2})/(\d{4})\b.*?\bau\s+(\d{2})/(\d{2})/(\d{4})', txt, re.I|re.S)
    if m:
        return int(m.group(3))
    m2 = re.search(r'\b(20\d{2})\b', txt)
    return int(m2.group(1)) if m2 else None

HOUR_RE = re.compile(r'(\d{1,2})\s*(?:h|heures?)\s*(\d{2})', re.I)

def _parse_times_from_aria(aria: str) -> Tuple[Optional[Tuple[int,int]], Optional[Tuple[int,int]]]:
    hhmm = HOUR_RE.findall(aria)
    if len(hhmm) >= 2:
        (h1,m1),(h2,m2) = hhmm[0], hhmm[1]
        return (int(h1),int(m1)), (int(h2),int(m2))
    return None, None

def _parse_day_month_from_aria(aria: str) -> Tuple[Optional[int], Optional[int]]:
    # ex: "Cours du 8 septembre de 9 heures 05 à 10 heures 00"
    m = re.search(r'\bdu\s+(\d{1,2})\s+([a-zA-Zéû\.]+)', _strip_accents(aria), re.I)
    if not m:
        return None, None
    d = int(m.group(1))
    mm = _month_fr_to_num(m.group(2))
    return (d, int(mm)) if mm else (None, None)

# ========= petites aides Playwright =========
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
        const t = (document.body.innerText||'').replace(/\s+/g,' ');
        const hasWeek = /du\s+\d{2}\/\d{2}\/\d{4}\s+au\s+\d{2}\/\d{2}\/\d{4}/i.test(t);
        const hasCourse = document.querySelector('div[id*="_coursInt_"][aria-label]') != null;
        return hasWeek || hasCourse;
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
        page.wait_for_timeout(400)
    raise TimeoutError("Timetable not found in any frame")

def click_text_anywhere(page, patterns: List[str]) -> bool:
    for frame in page.frames:
        for pat in patterns:
            for loc in [
                frame.get_by_role("link",   name=re.compile(pat, re.I)),
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

# ========= Navigation ENT -> PRONOTE =========
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

def _click_css_in_frames(page, css: str, frame_url_contains: str = "", screenshot_tag: str = "") -> bool:
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

def goto_timetable(pronote_page):
    pronote_page.set_default_timeout(TIMEOUT_MS)
    accept_cookies_any(pronote_page)

    # perso : "Vie scolaire" -> "Emploi du temps"
    if TIMETABLE_PRE_SELECTOR:
        _click_css_in_frames(pronote_page, TIMETABLE_PRE_SELECTOR, TIMETABLE_FRAME, "pre-selector")
    if TIMETABLE_SELECTOR:
        if _click_css_in_frames(pronote_page, TIMETABLE_SELECTOR, TIMETABLE_FRAME, "timetable-selector"):
            accept_cookies_any(pronote_page)
            try:
                fr2 = wait_timetable_any_frame(pronote_page, timeout_ms=30_000)
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-custom-selector.png", full_page=True)
                return fr2
            except TimeoutError:
                pass

    # déjà sur EDT ?
    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=10_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-already-here.png", full_page=True)
        return fr
    except TimeoutError:
        pass

    # heuristiques
    attempts = [
        ["Emploi du temps", "Mon emploi du temps", "Emplois du temps"],
        ["Planning", "Agenda"],
        ["Vie scolaire", "Emploi du temps"],
    ]
    for i, pats in enumerate(attempts, 1):
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
    return _click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}")

# ========= Extraction EDT (dans le frame) =========
def extract_week_info(pronote_page) -> Dict[str, Any]:
    """Retourne {header, year_hint, lessons:[{aria,subject,room}...]} à partir du frame EDT."""
    fr = wait_timetable_any_frame(pronote_page, timeout_ms=30_000)

    header_text = fr.evaluate("() => (document.body.innerText||'')")
    year_hint   = _parse_year_from_header(header_text)

    lessons = fr.evaluate(r"""
      () => {
        const items = [];
        const nodes = Array.from(document.querySelectorAll('div[id*="_coursInt_"][aria-label]'));
        for (const n of nodes) {
          const aria = n.getAttribute('aria-label') || '';

          // Lire les cellules cont0 / cont1 (texte affiché dans la case)
          const getLines = (id) => {
            const td = n.querySelector(`td[id*="${id}"]`);
            if (!td) return [];
            return Array.from(td.querySelectorAll('.NoWrap, .NoWrap.AlignementMilieu'))
              .map(x => (x.textContent||'').trim())
              .filter(Boolean);
          };
          const l0 = getLines('_cont0');
          const l1 = getLines('_cont1');

          // Matière probable
          const probableSubject = (l0[1] || l0[0] || l1[0] || '').trim();

          // Salle probable (codes courts : S11 / L12 / 004, etc.)
          let room = '';
          for (const s of [...l0, ...l1]) {
            if (/^(?:S\d{2,3}|[A-Z]{1,3}\d{1,3}|\d{3})$/.test(s)) { room = s; break; }
          }

          items.push({ aria, subject: probableSubject, room });
        }
        return items;
      }
    """)

    return {"header": header_text, "year_hint": year_hint, "lessons": lessons}

def build_entries_from_week(info: Dict[str,Any]) -> List[Dict[str,Any]]:
    """Convertit les leçons en entrées datées (start_dt/end_dt/title/room)."""
    entries: List[Dict[str,Any]] = []
    year = info.get("year_hint")
    for it in info.get("lessons", []):
        aria = it.get("aria") or ""
        subject = (it.get("subject") or "").strip()
        room = (it.get("room") or "").strip()

        t1, t2 = _parse_times_from_aria(aria)
        d, m   = _parse_day_month_from_aria(aria)

        if not (t1 and t2 and d and m and year):
            # cours incomplet → on ignore
            continue

        try:
            start_dt = datetime(year, m, d, t1[0], t1[1])
            end_dt   = datetime(year, m, d, t2[0], t2[1])
        except Exception:
            continue

        title = subject or "Cours"
        title = f"{TITLE_PREFIX}{title}"

        entries.append({
            "start_dt": start_dt,
            "end_dt": end_dt,
            "title": title,
            "room": room,
        })
    return entries

def merge_adjacent(entries: List[Dict[str,Any]], max_gap_minutes: int = 1) -> List[Dict[str,Any]]:
    """Fusionne les cases contiguës (même titre/salle/jour) si l'heure de début == heure de fin précédente (±max_gap)."""
    if not entries:
        return []
    es = sorted(entries, key=lambda e: (e["start_dt"], e["end_dt"], e["title"], e["room"]))
    out = [es[0].copy()]
    for e in es[1:]:
        prev = out[-1]
        same_day  = prev["start_dt"].date() == e["start_dt"].date()
        same_meta = (prev["title"] == e["title"] and (prev["room"] or "") == (e["room"] or ""))
        gap = (e["start_dt"] - prev["end_dt"]).total_seconds() / 60.0
        if same_day and same_meta and abs(gap) <= max_gap_minutes:
            # étendre
            if e["end_dt"] > prev["end_dt"]:
                prev["end_dt"] = e["end_dt"]
        else:
            out.append(e.copy())
    return out

# ========= MAIN =========
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

        # ENT → PRONOTE → Emploi du temps
        login_ent(page)
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # semaines à parcourir
        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1
        print(f"[CFG] Weeks: {start_idx}..{end_idx}")

        for week_idx in range(start_idx, end_idx + 1):
            # essaie via onglet j_n
            used_tab = goto_week_by_index(pronote, week_idx)
            accept_cookies_any(pronote)
            ensure_all_visible(pronote)

            info = extract_week_info(pronote)
            raw_entries = build_entries_from_week(info)
            print(f"Semaine {week_idx}: {len(raw_entries)} cours, header='{(info.get('header') or '').splitlines()[0] if info.get('header') else ''}'")
            print(f"[DBG]   entries construits: {len(raw_entries)}")

            merged = merge_adjacent(raw_entries)
            print(f"[DBG]   après fusion: {len(merged)}")

            for e in merged:
                # bornes de sécurité : -21 jours / +90 jours
                now = datetime.now()
                if e["end_dt"] < (now - timedelta(days=21)) or e["start_dt"] > (now + timedelta(days=90)):
                    continue

                hash_id = make_hash_id(e["start_dt"], e["end_dt"], e["title"], e["room"])
                body = {
                    "summary": e["title"],
                    "location": e["room"],
                    "start": {"dateTime": e["start_dt"].isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": e["end_dt"].isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"mo_hash": hash_id, "source": "pronote_playwright"}},
                }
                try:
                    action = upsert_event_by_hash(svc, CALENDAR_ID, hash_id, body)
                    if action == "created": created += 1
                    else: updated += 1
                except HttpError as e:
                    print(f"[GCAL] {e}")

            # fallback si pas d’onglet
            if not used_tab and week_idx < end_idx:
                if not click_first_in_frames(pronote, [
                    'button[title*="suivante"]','button[aria-label*="suivante"]',
                    'a[title*="suivante"]','a:has-text("Semaine suivante")'
                ]):
                    break
                accept_cookies_any(pronote)
                try:
                    wait_timetable_any_frame(pronote, timeout_ms=20_000)
                except TimeoutError:
                    pass

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
        print(f"[FATAL] {ex}")
        sys.exit(1)
