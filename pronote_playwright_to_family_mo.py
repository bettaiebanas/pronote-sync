# pronote_playwright_to_family_mo.py
# SPDX-License-Identifier: MIT
from __future__ import annotations

import os, re, sys, time, json, hashlib, unicodedata, random
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Union, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Frame
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ===================== Variables d'env (inchangées) =====================
ENT_URL       = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL   = os.getenv("PRONOTE_URL", "")
ENT_USER      = os.getenv("PRONOTE_USER", "")
ENT_PASS      = os.getenv("PRONOTE_PASS", "")

CALENDAR_ID   = os.getenv("CALENDAR_ID", "")
TITLE_PREFIX  = os.getenv("TITLE_PREFIX", "[Mo] ")  # ← mets "" pour ne plus ajouter [Mo]
COLOR_ID      = os.getenv("COLOR_ID", "6")
HEADFUL       = os.getenv("HEADFUL", "0") == "1"

TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()  # ex: 'parent.html'
WEEK_TAB_TEMPLATE      = os.getenv("WEEK_TAB_TEMPLATE", "#GInterface\\.Instances\\[2\\]\\.Instances\\[0\\]_j_{n}").strip()

FETCH_WEEKS_FROM       = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH         = int(os.getenv("WEEKS_TO_FETCH", "4"))
CLICK_TOUT_VOIR        = os.getenv("CLICK_TOUT_VOIR", "1") == "1"
WAIT_AFTER_NAV_MS      = int(os.getenv("WAIT_AFTER_NAV_MS", "1000"))

# Bornes anti-hang
MAX_TILES_PER_WEEK     = int(os.getenv("MAX_TILES_PER_WEEK", "120"))
MAX_CLICK_PER_SELECTOR = int(os.getenv("MAX_CLICK_PER_SELECTOR", "120"))
WEEK_HARD_TIMEOUT_MS   = int(os.getenv("WEEK_HARD_TIMEOUT_MS", "120000"))
PANEL_WAIT_MS          = int(os.getenv("PANEL_WAIT_MS", "350"))
PANEL_RETRIES          = int(os.getenv("PANEL_RETRIES", "8"))

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES           = ["https://www.googleapis.com/auth/calendar"]
TIMEZONE         = "Europe/Paris"

TIMEOUT_MS  = 120_000
SCREEN_DIR  = "screenshots"

# ===================== Console UTF-8 =====================
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try: print(f"[{ts}] {msg}")
    except UnicodeEncodeError: print(f"[{ts}] {msg}".encode("ascii","replace").decode("ascii"))

def _safe_mkdir(p: str) -> None:
    try: os.makedirs(p, exist_ok=True)
    except Exception: pass

def _safe_shot(page_or_frame: Union[Page, Frame], name: str) -> None:
    try:
        page = page_or_frame if isinstance(page_or_frame, Page) else page_or_frame.page
        _safe_mkdir(SCREEN_DIR)
        page.screenshot(path=f"{SCREEN_DIR}/{name}.png", full_page=True)
    except Exception: pass

def _safe_write(path: str, data: str, enc: str = "utf-8") -> None:
    try:
        _safe_mkdir(os.path.dirname(path) or ".")
        with open(path, "w", encoding=enc) as f: f.write(data)
    except Exception as e:
        log(f"[DEBUG] write fail {path}: {e}")

# ===================== Utils: RFC3339 Europe/Paris =====================
def _last_sunday(year: int, month: int) -> datetime:
    d = datetime(year, month, 31)
    while d.weekday() != 6:
        d -= timedelta(days=1)
    return d

def _paris_offset(dt: datetime) -> str:
    start = _last_sunday(dt.year, 3)
    end   = _last_sunday(dt.year, 10)
    return "+02:00" if start <= dt <= end else "+01:00"

def to_rfc3339_local(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + _paris_offset(dt)

# ===================== GCAL =====================
def get_gcal_service():
    if not CALENDAR_ID: raise SystemExit("CALENDAR_ID manquant.")
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
            log(f"[Google OAuth] {e}"); raise
        with open(TOKEN_FILE, "w", encoding="utf-8") as f: f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii","ignore").decode()
    return re.sub(r"\s+"," ",s).strip().lower()

# --- Cœur de titre (sans préfixe/statut) pour dédoublonnage stable
_STATUS_CANON = {
    "prof. absent": "Prof. absent",
    "prof absent": "Prof. absent",
    "absence professeur": "Prof. absent",
    "absence du professeur": "Prof. absent",
    "cours annulé": "Cours annulé",
    "cours annule": "Cours annulé",
    "annulation": "Cours annulé",
    "changement de salle": "Changement de salle",
    "cours modifié": "Cours modifié",
    "cours modifie": "Cours modifié",
}
_STATUS_RE = re.compile(r'\((?:prof\.?\s*absent|cours\s+annul[eé]|changement\s+de\s+salle|cours\s+modifi[eé])\)\s*$', re.I)

def _title_core(title: str) -> str:
    t = title or ""
    t = re.sub(r'^\s*\[[^\]]+\]\s*', '', t)  # enlève [XX] en tête
    t = _STATUS_RE.sub('', t)                # enlève (statut)
    return t.strip()

def make_dedupe_key(start: datetime, end: datetime, title: str, location: str) -> str:
    key = f"{start.isoformat()}|{end.isoformat()}|{_norm(_title_core(title))}|{_norm(location)}"
    return hashlib.sha1(key.encode()).hexdigest()

def _backoff_sleep(i: int): time.sleep(min(30, (2 ** i) + random.uniform(0, 0.5)))

def _parse_gcal_dt(ev_dt: Dict[str, str]) -> Optional[datetime]:
    s = ev_dt.get("dateTime") or ev_dt.get("date")
    if not s: return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        try: return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception: return None

def _find_existing_event(svc, cal_id: str, body: Dict[str, Any], title: str, location: str, dedupe_key: str):
    start = datetime.fromisoformat(body["start"]["dateTime"])
    end   = datetime.fromisoformat(body["end"]["dateTime"])
    # 1) par clé dedupe
    try:
        res = svc.events().list(
            calendarId=cal_id,
            privateExtendedProperty=f"dedupe={dedupe_key}",
            timeMin=to_rfc3339_local(start - timedelta(days=1)),
            timeMax=to_rfc3339_local(end + timedelta(days=1)),
            maxResults=5, singleEvents=True, showDeleted=False
        ).execute()
        items = res.get("items", [])
        if items: return items[0]
    except HttpError: pass
    # 2) compat : même source, cœur titre + salle + créneau ~
    try:
        res = svc.events().list(
            calendarId=cal_id,
            privateExtendedProperty="source=pronote_playwright",
            timeMin=to_rfc3339_local(start - timedelta(hours=6)),
            timeMax=to_rfc3339_local(end + timedelta(hours=6)),
            maxResults=50, singleEvents=True, showDeleted=False
        ).execute()
        cand = res.get("items", [])
    except HttpError:
        cand = []
    core = _norm(_title_core(title)); locn = _norm(location)
    for ev in cand:
        escore = _norm(_title_core(ev.get("summary",""))); eloc = _norm(ev.get("location",""))
        estart = _parse_gcal_dt(ev.get("start", {}));     eend = _parse_gcal_dt(ev.get("end", {}))
        if not (estart and eend): continue
        if abs((estart - start).total_seconds()) <= 600 and abs((eend - end).total_seconds()) <= 600 and escore == core and eloc == locn:
            return ev
    return None

def upsert_event_by_dedupe(svc, cal_id: str, body: Dict[str, Any], dedupe_key: str) -> Tuple[str, Dict[str, Any]]:
    existing = _find_existing_event(svc, cal_id, body, body["summary"], body.get("location",""), dedupe_key)
    ext = body.setdefault("extendedProperties", {}).setdefault("private", {})
    ext["source"] = "pronote_playwright"
    ext["dedupe"] = dedupe_key
    for i in range(4):
        try:
            if existing:
                ev = svc.events().patch(calendarId=cal_id, eventId=existing["id"], body=body, sendUpdates="none").execute()
                return "updated", ev
            else:
                ev = svc.events().insert(calendarId=cal_id, body=body, sendUpdates="none").execute()
                return "created", ev
        except HttpError as e:
            if getattr(e, "res", None) and e.res.status == 403 and "rateLimitExceeded" in str(e):
                log("[GCAL] Rate limit — retry..."); _backoff_sleep(i); continue
            raise

# ===================== Parsing =====================
H_PATTERNS = [
    re.compile(r'(?P<h>\d{1,2})\s*[hH:]\s*(?P<m>\d{2})'),
    re.compile(r'(?P<h>\d{1,2})\s*(?:heures?|hrs?)\s*(?P<m>\d{2})', re.IGNORECASE)
]
DURATION_RE = re.compile(r'(?P<dh>\d{1,2})\s*[hH]\s*(?P<dm>\d{2})')

def parse_times(text: str) -> Dict[str, Optional[tuple[int,int]]]:
    t = (text or "").strip()
    hours: List[tuple[int,int]] = []
    for rx in H_PATTERNS:
        for m in rx.finditer(t):
            hours.append((int(m.group("h")), int(m.group("m"))))
    if 'à' in t and re.search(r'\d{1,2}\s*/\s*\d{1,2}(?:\s*/\s*\d{2,4})?', t) and len(hours) == 2:
        return {"start": hours[1], "end": None, "duration": hours[0]}
    if len(hours) >= 2:
        return {"start": hours[0], "end": hours[1], "duration": None}
    dm = DURATION_RE.search(t)
    if dm and hours:
        return {"start": hours[-1], "end": None, "duration": (int(dm.group("dh")), int(dm.group("dm")))}
    return {"start": None, "end": None, "duration": None}

MONTHS_FR = {
    "janvier":1, "février":2, "fevrier":2, "mars":3, "avril":4, "mai":5, "juin":6,
    "juillet":7, "août":8, "aout":8, "septembre":9, "octobre":10, "novembre":11, "décembre":12, "decembre":12
}

def parse_date_from_text(text: str, fallback_year: int) -> Optional[datetime]:
    m = re.search(r'(\d{1,2})\s*/\s*(\d{1,2})(?:\s*/\s*(\d{2,4}))?', text or '')
    if m:
        d, mo = int(m.group(1)), int(m.group(2))
        y = int(m.group(3)) if m.group(3) else fallback_year
        if y < 100: y += 2000
        return datetime(y, mo, d)
    m = re.search(r'(\d{1,2})\s+([A-Za-zéûùôîïàâç]+)', text or '', re.IGNORECASE)
    if m:
        d = int(m.group(1)); mo_name = m.group(2).lower()
        mo = MONTHS_FR.get(mo_name)
        if mo:
            return datetime(fallback_year, mo, d)
    return None

def to_dt(date_base: datetime, hm: tuple[int,int]) -> datetime:
    return date_base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

def parse_panel(panel: Dict[str, Any], year: int) -> Optional[Dict[str, Any]]:
    header = panel.get("header","")
    matiere = re.sub(r'\s+', ' ', (panel.get("matiere","") or "").strip())
    salle   = re.sub(r'\s+', ' ', (panel.get("salle","") or "").strip())
    times = parse_times(header)
    if not (times["start"] or times["end"]): return None
    dt_date = parse_date_from_text(header, fallback_year=year)
    if not dt_date: return None
    start_hm = times["start"]; end_hm = times["end"]
    if start_hm and end_hm:
        start_dt = to_dt(dt_date, start_hm); end_dt = to_dt(dt_date, end_hm)
    elif start_hm and times["duration"]):
        dh, dm = times["duration"]; start_dt = to_dt(dt_date, start_hm); end_dt = start_dt + timedelta(hours=dh, minutes=dm)
    else:
        return None
    summary = matiere or "Cours"
    if not salle:
        m = re.search(r'(?:Salle[s]?\s+)(.+)$', header, re.IGNORECASE)
        if m: salle = m.group(1).strip()
    return {"summary": summary, "room": salle, "start_dt": start_dt, "end_dt": end_dt}

# ===================== Playwright helpers =====================
def _iter_contexts(page: Page):
    yield page
    for fr in page.frames: yield fr

def first_locator_any(page: Page, selectors: List[str]):
    for ctx in _iter_contexts(page):
        try:
            for sel in selectors:
                loc = ctx.locator(sel)
                if loc.count() > 0: return loc.first
        except Exception:
            continue
    return None

def click_first_any(page: Page, selectors: List[str]) -> bool:
    loc = first_locator_any(page, selectors)
    if not loc: return False
    try:
        loc.click(); return True
    except Exception:
        try:
            el = loc.element_handle()
            if el: el.evaluate("(n)=>n.click()"); return True
        except Exception: pass
        return False

def accept_cookies_any(page: Page) -> None:
    texts = ["Tout accepter","Accepter tout","J'accepte","Accepter","OK","Continuer","J'ai compris"]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_any(page, sels)

def _frame_has_timetable_js() -> str:
    return r"""
      () => {
        const txt = (document.body.innerText || '').replace(/\s+/g,' ');
        const hasTitle = /Emploi du temps/i.test(txt) || /Planning|Agenda/i.test(txt);
        const hasWeek  = /(Semaine|du\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\s+au\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)/i.test(txt);
        const hasTimes = /\d{1,2}\s*[h:]\s*\d{2}|heures?\s*\d{2}/i.test(txt);
        return (hasTitle && (hasTimes || hasWeek)) || (hasWeek && hasTimes);
      }
    """

def _frame_has_dom_grid_js() -> str:
    return r"""
      () => {
        const q1 = document.querySelectorAll('[id^="id_"][id*="_coursInt_"]').length;
        const q2 = document.querySelectorAll('[id^="id_"][id*="_cont"]').length;
        const q3 = document.querySelectorAll('.EnteteCoursLibelle').length;
        return (q1 + q2 + q3) > 0;
      }
    """

def find_timetable_ctx(page: Page, timeout_ms: int = TIMEOUT_MS) -> Union[Page, Frame]:
    deadline = time.time() + timeout_ms/1000.0
    while time.time() < deadline:
        if TIMETABLE_FRAME:
            for fr in page.frames:
                try:
                    if TIMETABLE_FRAME in (fr.url or "") or TIMETABLE_FRAME in (fr.name or ""):
                        if fr.evaluate(_frame_has_timetable_js()): 
                            return fr
                except Exception: 
                    continue
        for fr in page.frames:
            try:
                if fr.evaluate(_frame_has_timetable_js()):
                    return fr
            except Exception: 
                continue
        try:
            if page.evaluate(_frame_has_timetable_js()):
                return page
        except Exception: 
            pass
        page.wait_for_timeout(250)
    raise TimeoutError("Timetable context not found")

def find_dom_grid_ctx(page: Page, prefer: Optional[Union[Page, Frame]] = None, timeout_ms: int = 5000) -> Optional[Union[Page, Frame]]:
    end = time.time() + timeout_ms/1000.0
    while time.time() < end:
        cand = []
        if prefer: cand.append(prefer)
        cand.extend(list(page.frames))
        cand.append(page)
        for ctx in cand:
            try:
                if ctx.evaluate(_frame_has_dom_grid_js()):
                    return ctx
            except Exception:
                continue
        page.wait_for_timeout(250)
    return None

def wait_timetable_any(page: Page, timeout_ms: int = TIMEOUT_MS) -> Union[Page, Frame]:
    return find_timetable_ctx(page, timeout_ms)

def click_css_any(page_or_frame: Union[Page, Frame], css: str, screenshot_tag: str = "") -> bool:
    if not css: return False
    ctx = page_or_frame
    try:
        loc = ctx.locator(css)
        if loc.count() > 0:
            try: loc.first.scroll_into_view_if_needed()
            except Exception: pass
            try: loc.first.click()
            except Exception:
                try:
                    el = loc.first.element_handle()
                    if el: el.evaluate("(n)=>{ n.click(); n.dispatchEvent(new MouseEvent('mousedown',{bubbles:true})); n.dispatchEvent(new MouseEvent('mouseup',{bubbles:true})); n.dispatchEvent(new MouseEvent('click',{bubbles:true})); }")
                    else: return False
                except Exception: return False
            (ctx.page if isinstance(ctx, Frame) else ctx).wait_for_timeout(WAIT_AFTER_NAV_MS)
            if screenshot_tag: _safe_shot(ctx, f"08-clicked-{screenshot_tag}")
            return True
    except Exception as e:
        log(f"[NAV] click_css_any fail: {e}")
    return False

# ===================== Navigation =====================
def login_ent(page: Page) -> None:
    _safe_mkdir(SCREEN_DIR)
    page.set_default_timeout(TIMEOUT_MS)
    page.goto(ENT_URL)
    page.wait_for_load_state("load")
    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    _safe_shot(page, "01-ent-welcome")

    click_first_any(page, [
        'a:has-text("Se connecter")','a:has-text("Connexion")',
        'button:has-text("Se connecter")','button:has-text("Connexion")',
        'a[href*="login"]','a[href*="auth"]'
    ])
    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    _safe_shot(page, "02-ent-after-click-login")

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

    user_loc = first_locator_any(page, user_candidates)
    pass_loc = first_locator_any(page, pass_candidates)
    if not user_loc or not pass_loc:
        click_first_any(page, ['button:has-text("Identifiant")','a:has-text("Identifiant")','button:has-text("Compte")','a:has-text("Compte")','a:has-text("ENT")'])
        page.wait_for_load_state("domcontentloaded")
        accept_cookies_any(page)
        user_loc = first_locator_any(page, user_candidates)
        pass_loc = first_locator_any(page, pass_candidates)

    if not user_loc or not pass_loc:
        _safe_shot(page, "03-ent-no-fields")
        raise RuntimeError("Champ identifiant ENT introuvable.")

    user_loc.fill(ENT_USER); pass_loc.fill(ENT_PASS)
    if not click_first_any(page, submit_candidates): user_loc.press("Enter")
    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    _safe_shot(page, "05-ent-after-submit")

def open_pronote(context, page: Page):
    page.set_default_timeout(TIMEOUT_MS)
    if PRONOTE_URL:
        page.goto(PRONOTE_URL)
        page.wait_for_load_state("load")
        page.wait_for_load_state("domcontentloaded")
        accept_cookies_any(page)
        _safe_shot(page, "06-pronote-direct")
        return page

    with page.expect_popup() as p:
        clicked = click_first_any(page, [
            'a:has-text("PRONOTE")','a[title*="PRONOTE"]','a[href*="pronote"]','text=PRONOTE'
        ])
        if not clicked:
            _safe_shot(page, "06-pronote-tile-not-found")
            raise RuntimeError("Tuile PRONOTE introuvable.")
    try:
        pronote_page = p.value
        pronote_page.wait_for_load_state("domcontentloaded")
    except PWTimeout:
        pronote_page = page
        pronote_page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(pronote_page)
    _safe_shot(pronote_page, "07-pronote-home")
    return pronote_page

def goto_timetable(pronote_page: Page) -> Union[Page, Frame]:
    pronote_page.set_default_timeout(TIMEOUT_MS)
    accept_cookies_any(pronote_page)

    if TIMETABLE_PRE_SELECTOR:
        click_css_any(pronote_page, TIMETABLE_PRE_SELECTOR, "pre-selector")
    if TIMETABLE_SELECTOR:
        if click_css_any(pronote_page, TIMETABLE_SELECTOR, "timetable-selector"):
            accept_cookies_any(pronote_page)
            try:
                ctx = find_timetable_ctx(pronote_page, timeout_ms=30_000)
            except TimeoutError:
                ctx = pronote_page
            grid = find_dom_grid_ctx(pronote_page, prefer=ctx, timeout_ms=5000) or ctx
            _safe_shot(grid, "08-timetable-custom-selector")
            return grid

    try:
        ctx = find_timetable_ctx(pronote_page, timeout_ms=10_000)
    except TimeoutError:
        ctx = pronote_page
    grid = find_dom_grid_ctx(pronote_page, prefer=ctx, timeout_ms=5000) or ctx
    _safe_shot(grid, "08-timetable-already-here")
    return grid

def ensure_all_visible(ctx: Union[Page, Frame]) -> None:
    if CLICK_TOUT_VOIR:
        click_css_any(ctx, '*:has-text("Tout voir")', "tout-voir") or \
        click_css_any(ctx, '*:has-text("Voir tout")', "voir-tout") or \
        click_css_any(ctx, '*:has-text("Tout afficher")', "tout-afficher")
        (ctx.page if isinstance(ctx, Frame) else ctx).wait_for_timeout(250)

def goto_week_by_index(pronote_page: Page, current_ctx: Union[Page, Frame], n: int) -> Union[Page, Frame]:
    if not WEEK_TAB_TEMPLATE:
        return current_ctx
    css = WEEK_TAB_TEMPLATE.format(n=n)
    click_css_any(current_ctx, css, f"week-{n}")
    grid = find_dom_grid_ctx(pronote_page, prefer=current_ctx, timeout_ms=5000) or current_ctx
    # Attente robuste que la grille soit présente
    end = time.time() + WEEK_HARD_TIMEOUT_MS/1000.0
    while time.time() < end:
        try:
            cnt = grid.evaluate("""() => {
                const a = document.querySelectorAll('[id^="id_"][id*="_coursInt_"]').length;
                const b = document.querySelectorAll('[id^="id_"][id*="_cont"]').length;
                const c = document.querySelectorAll('.EnteteCoursLibelle').length;
                return a+b+c;
            }""")
            if int(cnt or 0) > 0:
                break
        except Exception:
            pass
        (pronote_page.page if isinstance(pronote_page, Frame) else pronote_page).wait_for_timeout(300)
        grid = find_dom_grid_ctx(pronote_page, prefer=grid, timeout_ms=1500) or grid
    return grid

# ===================== Extraction =====================
def _list_course_ids(ctx: Union[Page, Frame]) -> List[str]:
    try:
        ids = ctx.evaluate(r"""() => {
          const pos = (e) => (e?.getBoundingClientRect()?.top || 9e9);
          const uniq = {};
          for (const e of document.querySelectorAll('[id^="id_"][id*="_coursInt_"]')) uniq[e.id] = pos(e);
          for (const e of document.querySelectorAll('[id^="id_"][id*="_cont"]')) {
            const t = pos(e);
            uniq[e.id] = Math.min(uniq[e.id] ?? t, t);
          }
          return Object.entries(uniq).sort((a,b)=>a[1]-b[1]).map(x=>x[0]);
        }""")
        return ids or []
    except Exception:
        return []

def _click_by_id(ctx: Union[Page, Frame], el_id: str) -> bool:
    return ctx.evaluate("""(id)=>{
      const el = document.getElementById(id);
      if(!el) return false;
      el.scrollIntoView({block:'center'});
      try { el.click(); } catch(e) {}
      try {
        el.dispatchEvent(new MouseEvent('mousedown',{bubbles:true}));
        el.dispatchEvent(new MouseEvent('mouseup',{bubbles:true}));
        el.dispatchEvent(new MouseEvent('click',{bubbles:true}));
      } catch(e){}
      return true;
    }""", el_id)

def _read_visible_panel(ctx: Union[Page, Frame]) -> Optional[Dict[str, Any]]:
    for _ in range(PANEL_RETRIES):
        panel = ctx.evaluate(r"""() => {
          const panels = Array.from(document.querySelectorAll('.ConteneurCours'));
          if (!panels.length) return null;
          const p = panels[panels.length-1];
          const header = (p.querySelector('.EnteteCoursLibelle')?.innerText||'').replace(/\s+/g,' ').trim();
          const raw    = (p.innerText||'').replace(/\s+/g,' ').trim();
          const groups = Array.from(p.querySelectorAll('[role="group"]'));
          const pick = (name) => {
            const g = groups.find(x => (x.getAttribute('aria-label')||'').toLowerCase().includes(name));
            return g ? (g.innerText||'').replace(/\s+/g,' ').trim() : '';
          };
          return header ? { header, raw, matiere: pick('matière') || pick('matiere'), salle: pick('salles') || pick('salle') } : null;
        }""")
        if panel: 
            return panel
        time.sleep(PANEL_WAIT_MS/1000.0)
    return None

def _collect_pairs_by_proximity(ctx: Union[Page, Frame]) -> List[Dict[str, str]]:
    return ctx.evaluate(r"""() => {
      const cours = Array.from(document.querySelectorAll('[id^="id_"][id*="_coursInt_"]')).map(e=>({id:e.id, r:e.getBoundingClientRect()}));
      const conts = Array.from(document.querySelectorAll('[id^="id_"][id*="_cont"]')).map(e=>({id:e.id, r:e.getBoundingClientRect(), text:(e.innerText||'').replace(/\s+/g,' ').trim()}));
      const byBase = (s) => (s.match(/^id_(\d+)_/)||[])[1]||'';
      const groupCont = {};
      for (const c of conts) {
        const b = byBase(c.id);
        (groupCont[b] = groupCont[b] || []).push(c);
      }
      const out = [];
      for (const cu of cours) {
        const b = byBase(cu.id);
        const list = (groupCont[b]||[]);
        if (!list.length) { out.push({id:cu.id, aria:'', cont:''}); continue; }
        let best = list[0], bestd = 1e12;
        for (const x of list) {
          const d = Math.abs((x.r.top + x.r.bottom)/2 - (cu.r.top + cu.r.bottom)/2);
          if (d < bestd) { best=x; bestd=d; }
        }
        out.push({id:cu.id, aria:(document.getElementById(cu.id)?.getAttribute('aria-label')||''), cont: best.text});
      }
      return out;
    }""")

def _read_week_header(ctx: Union[Page, Frame]) -> str:
    try:
        return ctx.evaluate(r"""() => {
          const txt = (document.body.innerText || '').replace(/\s+/g,' ');
          const m = txt.match(/du\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\s+au\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?/i);
          return m ? m[0] : '';
        }""")
    except Exception:
        return ""

def extract_week_info(ctx: Union[Page, Frame]) -> Dict[str, Any]:
    header_text = _read_week_header(ctx)
    monday: Optional[datetime] = None
    try:
        m = re.search(r'du\s+(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?', header_text or '', flags=re.IGNORECASE)
        if m:
            y = int(m.group(3)) if m.group(3) else datetime.now().year
            if y < 100: y += 2000
            monday = datetime(y, int(m.group(2)), int(m.group(1)))
    except Exception:
        monday = None

    tiles: List[Dict[str, Any]] = []
    year = (monday.year if monday else datetime.now().year)

    counts = {}
    for sel in ['[id^="id_"][id*="_coursInt_"] table','[id^="id_"][id*="_coursInt_"]','[id^="id_"][id*="_cont"]']:
        try: c = ctx.evaluate("(s)=>document.querySelectorAll(s).length", sel)
        except Exception: c = 0
        counts[sel] = int(c or 0)
    _safe_write(f"{SCREEN_DIR}/edp_selector_counts.json", json.dumps(counts, ensure_ascii=False, indent=2))

    click_log = []
    ids = _list_course_ids(ctx)
    lim = min(len(ids), MAX_TILES_PER_WEEK)
    for i in range(lim):
        el_id = ids[i]
        ok = _click_by_id(ctx, el_id)
        if not ok:
            click_log.append({"id": el_id, "clicked": False}); continue
        panel = _read_visible_panel(ctx)
        if not panel:
            click_log.append({"id": el_id, "clicked": True, "panel": None}); continue
        parsed = parse_panel(panel, year)
        click_log.append({"id": el_id, "clicked": True, "panel_header": panel.get("header",""), "parsed_ok": bool(parsed)})
        if not parsed: continue
        tiles.append({
            "label": f"{parsed['summary']} — {panel.get('header','')}",
            "summary": parsed["summary"],
            "room": parsed["room"],
            "start_dt": parsed["start_dt"],
            "end_dt": parsed["end_dt"],
            "panel_header": panel.get("header",""),
            "panel_text": panel.get("raw",""),
        })
        try: ctx.evaluate("()=>document.body.click()")
        except Exception: pass
        if len(tiles) >= MAX_TILES_PER_WEEK: break

    _safe_write(f"{SCREEN_DIR}/edp_click_log.json", json.dumps(click_log, ensure_ascii=False, indent=2))

    if not tiles:
        panels = ctx.evaluate(r"""() => {
          const list = [];
          const panels = Array.from(document.querySelectorAll('.ConteneurCours'));
          for (const p of panels) {
            const header = (p.querySelector('.EnteteCoursLibelle')?.innerText||'').replace(/\s+/g,' ').trim();
            if (!header) continue;
            const raw = (p.innerText||'').replace(/\s+/g,' ').trim();
            const groups = Array.from(p.querySelectorAll('[role="group"]'));
            const pick = (name) => {
              const g = groups.find(x => (x.getAttribute('aria-label')||'').toLowerCase().includes(name));
              return g ? (g.innerText||'').replace(/\s+/g,' ').trim() : '';
            };
            list.append({ header, raw, matiere: pick('matière') || pick('matiere'), salle: pick('salles') || pick('salle') })
          }
          return list;
        }""")
        for panel in (panels or []):
            parsed = parse_panel(panel, year)
            if not parsed: continue
            tiles.append({
                "label": f"{parsed['summary']} — {panel.get('header','')}",
                "summary": parsed["summary"],
                "room": parsed["room"],
                "start_dt": parsed["start_dt"],
                "end_dt": parsed["end_dt"],
                "panel_header": panel.get("header",""),
                "panel_text": panel.get("raw",""),
            })
            if len(tiles) >= MAX_TILES_PER_WEEK: break

    if not tiles:
        pairs = _collect_pairs_by_proximity(ctx)
        _safe_write(f"{SCREEN_DIR}/edp_pairs_preview.json", json.dumps(pairs[:20], ensure_ascii=False, indent=2))
        for t in pairs:
            aria = t.get("aria",""); cont = t.get("cont","")
            times = parse_times(aria)
            if not (times["start"] or times["end"]): continue
            dt_date = parse_date_from_text(aria, fallback_year=year)
            if not dt_date and monday:
                jours = ['lundi','mardi','mercredi','jeudi','vendredi','samedi','dimanche']
                found = next((i for i,n in enumerate(jours) if n in (aria or '').lower()), None)
                if found is not None: dt_date = monday + timedelta(days=found)
            if not dt_date: continue
            start_hm = times["start"]; end_hm = times["end"]
            if start_hm and end_hm:
                start_dt = to_dt(dt_date, start_hm); end_dt = to_dt(dt_date, end_hm)
            elif start_hm and times["duration"]):
                dh, dm = times["duration"]; start_dt = to_dt(dt_date, start_hm); end_dt = start_dt + timedelta(hours=dh, minutes=dm)
            else:
                continue
            summary = (re.sub(r'\s+',' ', cont).strip() or "Cours")
            room = ""
            m = re.search(r'(?:Salle[s]?\s+)(.+)$', cont, re.IGNORECASE)
            if m: room = m.group(1).strip()
            tiles.append({
                "label": f"{summary} — {aria}",
                "summary": summary,
                "room": room,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "panel_header": aria,
                "panel_text": aria + " " + cont,
            })
            if len(tiles) >= MAX_TILES_PER_WEEK: break

    if not tiles:
        try: html_full = ctx.evaluate("() => document.documentElement.outerHTML")
        except Exception: html_full = ""
        _safe_write(f"{SCREEN_DIR}/edp_full_dom.html", html_full)
        ids_dump = ctx.evaluate(r"""() => ({
          cours: Array.from(document.querySelectorAll('[id^="id_"][id*="_coursInt_"]')).map(e=>e.id),
          conts: Array.from(document.querySelectorAll('[id^="id_"][id*="_cont"]')).map(e=>e.id),
          entetes: Array.from(document.querySelectorAll('.EnteteCoursLibelle')).map(e=>e.innerText.trim()).slice(0,50)
        })""")
        _safe_write(f"{SCREEN_DIR}/edp_candidates.json", json.dumps(ids_dump, ensure_ascii=False, indent=2))

    _safe_write(f"{SCREEN_DIR}/edp_debug_summary.json", json.dumps({
        "header": header_text, 
        "monday": monday.isoformat() if monday else None,
        "click_ids": ids[:lim] if ids else [],
        "total_tiles": len(tiles)
    }, ensure_ascii=False, indent=2))

    return {"monday": monday, "tiles": tiles, "header": header_text}

# ===================== Main =====================
def run() -> None:
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("PRONOTE_USER / PRONOTE_PASS manquants.")

    svc = get_gcal_service()
    try:    me_primary = svc.calendars().get(calendarId="primary").execute()
    except: me_primary = {}
    try:    cal_meta = svc.calendars().get(calendarId=CALENDAR_ID).execute()
    except Exception as e: cal_meta = {"error": str(e)}

    _safe_write(f"{SCREEN_DIR}/gcal_whoami.json", json.dumps({"primary": me_primary, "target_calendar": cal_meta}, ensure_ascii=False, indent=2))
    log(f"[GCAL] Using calendar '{cal_meta.get('summary','?')}' (id={CALENDAR_ID}) as {me_primary.get('id','?')}")

    created = updated = 0
    created_events_dump: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id=TIMEZONE)
        page = context.new_page(); page.set_default_timeout(TIMEOUT_MS)

        log("Connexion ENT..."); login_ent(page)
        log("Ouverture PRONOTE..."); pronote = open_pronote(context, page)
        log("Navigation vers 'Emploi du temps'..."); ctx = goto_timetable(pronote)

        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1

        overall_min_dt: Optional[datetime] = None
        overall_max_dt: Optional[datetime] = None

        for week_idx in range(start_idx, end_idx + 1):
            log(f"-> Selection Semaine index={week_idx} via css '{WEEK_TAB_TEMPLATE.format(n=week_idx)}'")
            ctx = goto_week_by_index(pronote, ctx, week_idx)
            accept_cookies_any(pronote); ensure_all_visible(ctx)
            _safe_shot(ctx, f"08-week-{week_idx}-after-select")

            info  = extract_week_info(ctx)
            tiles = info["tiles"] or []
            hdr   = (info.get("header") or "").replace("\\n", " ")[:160]
            log(f"Semaine {week_idx}: {len(tiles)} cases, header='{hdr}'")

            for t in tiles:
                start_dt = t["start_dt"]; end_dt = t["end_dt"]
                summary  = t.get("summary") or "Cours"
                room     = t.get("room","")
                now = datetime.now()
                if end_dt < (now - timedelta(days=60)) or start_dt > (now + timedelta(days=180)): continue

                # Statut: cherche dans texte du panneau
                ph = " ".join([t.get("panel_text","") or "", t.get("panel_header","") or "", t.get("label","") or ""]).lower()
                status_tag = ""
                for k, canon in _STATUS_CANON.items():
                    if k in ph: status_tag = f" ({canon})"; break

                title    = f"{TITLE_PREFIX}{summary}{status_tag}"
                dedupe   = make_dedupe_key(start_dt, end_dt, title, room)
                body = {
                    "summary": title,
                    "location": room,
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": TIMEZONE},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": TIMEZONE},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"source": "pronote_playwright", "dedupe": dedupe}}
                }
                try:
                    action, ev = upsert_event_by_dedupe(svc, CALENDAR_ID, body, dedupe)
                    if action == "created": created += 1
                    else: updated += 1
                    created_events_dump.append({
                        "action": action, "summary": ev.get("summary"),
                        "start": ev.get("start"), "end": ev.get("end"),
                        "htmlLink": ev.get("htmlLink"), "id": ev.get("id"),
                    })
                except HttpError as e:
                    log(f"[GCAL] {e}")

                overall_min_dt = min(overall_min_dt or start_dt, start_dt)
