# pronote_playwright_to_family_mo.py
import os, sys, re, hashlib, unicodedata, time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG =========
ENT_URL       = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL   = os.getenv("PRONOTE_URL", "")       # si vide, on clique la tuile PRONOTE depuis l’ENT
ENT_USER      = os.getenv("PRONOTE_USER", "")
ENT_PASS      = os.getenv("PRONOTE_PASS", "")

TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()

WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "2"))  # remplace l’ancienne constante si tu en avais une

WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

CALENDAR_ID   = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX  = "[Mo] "
COLOR_ID      = "6"                                 # 6 = orange
WEEKS_TO_FETCH = int(os.getenv("WEEKS_TO_FETCH", "6"))  # ≈ 1 mois+
HEADFUL       = os.getenv("HEADFUL", "0") == "1"

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts (ms)
TIMEOUT_MS  = 120_000
SCREEN_DIR  = "screenshots"  # captures auto (uploadées en artifact)

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
    return build("calendar", "v3", credentials=creds)

# ---- Dédup béton : eventId stable (au lieu de dépendre d’une recherche) ----
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def make_event_id(start: datetime, end: datetime, title: str, location: str) -> str:
    key = f"{start.isoformat()}|{end.isoformat()}|{_norm(title)}|{_norm(location)}"
    # id Google doit être [a-z0-9_-] max ~1024 ; on fait court/stable
    return "prn_" + hashlib.sha1(key.encode()).hexdigest()[:24]

def upsert_event_by_id(svc, cal_id: str, event_id: str, body: Dict[str, Any]) -> str:
    try:
        # existe ? → patch
        svc.events().get(calendarId=cal_id, eventId=event_id).execute()
        svc.events().patch(calendarId=cal_id, eventId=event_id, body=body, sendUpdates="none").execute()
        return "updated"
    except HttpError as e:
        if getattr(e, "resp", None) and e.resp.status == 404:
            # n'existe pas → insert en imposant le même eventId
            svc.events().insert(calendarId=cal_id, body=body, eventId=event_id, sendUpdates="none").execute()
            return "created"
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

def accept_cookies_any(page):
    texts = [
        "Tout accepter","Accepter tout","J'accepte","Accepter",
        "OK","Continuer","J’ai compris","J'ai compris"
    ]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_in_frames(page, sels)

def _frame_has_timetable_js():
    # Détection permissive : texte “Emploi du temps” + empreintes d’horaires / période
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
    """Retourne le frame qui contient l’EDT (ou lève un TimeoutError)."""
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
    """Clique un élément cliquable dont le texte matche un pattern, dans n’importe quel frame."""
    for frame in page.frames:
        for pat in patterns:
            # 1) rôles accessibles
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
            # 2) fallback JS
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

def click_css_in_frames(page, css: str, frame_url_contains: str = "", screenshot_tag: str = "") -> bool:
    """
    Clique le premier élément correspondant au sélecteur CSS dans n'importe quel frame.
    Si frame_url_contains est non vide, ne clique que dans les frames dont l'URL contient cette sous-chaîne.
    """
    if not css:
        return False
    for fr in page.frames:
        if frame_url_contains and frame_url_contains not in fr.url:
            continue
        try:
            loc = fr.locator(css)
            if loc.count() > 0:
                loc.first.click()
                if screenshot_tag:
                    try: page.screenshot(path=f"{SCREEN_DIR}/08-clicked-{screenshot_tag}.png", full_page=True)
                    except: pass
                return True
        except Exception as e:
            print(f"[NAV] click_css_in_frames fail in {fr.url}: {e}")
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
     # 0) Ton chemin “perso” : Vie scolaire -> Emploi du temps
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
                # on tombera sur les heuristiques existantes plus bas

    os.makedirs(SCREEN_DIR, exist_ok=True)

    # 0) Déjà sur l’EDT ?
    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=10_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-already-here.png", full_page=True)
        return fr
    except TimeoutError:
        pass

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

    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=15_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-ready-fallback.png", full_page=True)
        return fr
    except TimeoutError:
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-NOT-found.png", full_page=True)
        raise RuntimeError("Impossible d’atteindre l’Emploi du temps (après multiples essais).")

def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        # plusieurs variantes possibles selon les instances PRONOTE
        click_text_anywhere(page, ["Tout voir", "Voir tout", "Tout afficher"])
        page.wait_for_timeout(400)

def goto_week_by_index(page, n: int) -> bool:
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)
    ok = click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}")
    if ok:
        try:
            wait_timetable_any_frame(page, timeout_ms=20_000)
        except TimeoutError:
            pass
    return ok

def extract_week_info(pronote_page) -> Dict[str, Any]:
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

    tiles = pronote_page.evaluate(r"""
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
        if (t && rx.test(t) && t.length < 160) add(e, t);
      });
      return out;
    }
    """)
    return {"monday": d0, "tiles": tiles, "header": header_text}

def iter_next_week(pronote_page) -> bool:
    if click_first_in_frames(pronote_page, [
        'button[title*="suivante"]','button[aria-label*="suivante"]','button:has-text("→")',
        'a[title*="suivante"]','a:has-text("Semaine suivante")'
    ]):
        accept_cookies_any(pronote_page)
        wait_for_timetable_ready(pronote_page)
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
        page.set_default_timeout(TIMEOUT_MS)

        # ENT → PRONOTE
        login_ent(page)
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # --- Parcours des semaines via tes onglets j_n ---
        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1

        for week_idx in range(start_idx, end_idx + 1):
            used_tab = goto_week_by_index(pronote, week_idx)
            accept_cookies_any(pronote)
            ensure_all_visible(pronote)

            info  = extract_week_info(pronote)
            d0    = info["monday"]
            tiles = info["tiles"] or []
            hdr   = (info.get("header") or "").replace("\n", " ")[:120]
            print(f"Semaine {week_idx}: {len(tiles)} cases, header='{hdr}'")

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
                hash_id = make_hash_id(start_dt, end_dt, title, parsed["room"])

                event = {
                    "summary": title,
                    "location": parsed["room"],
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

            if not used_tab and week_idx < end_idx:
                if not iter_next_week(pronote):
                    break

        browser.close()

    print(f"Terminé. créés={created}, maj={updated}")


        # Parcours n semaines
# --- Parcours des semaines via tes onglets j_n ---
start_idx = max(1, FETCH_WEEKS_FROM)
end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1

for week_idx in range(start_idx, end_idx + 1):
    # Essaie d'aller directement sur j_1, j_2, j_3…
    used_tab = goto_week_by_index(pronote, week_idx)

    accept_cookies_any(pronote)
    ensure_all_visible(pronote)  # clique “Tout voir” si activé

    info  = extract_week_info(pronote)
    d0    = info["monday"]
    tiles = info["tiles"] or []
    hdr   = (info.get("header") or "").replace("\n", " ")[:120]
    print(f"Semaine {week_idx}: {len(tiles)} cases, header='{hdr}'")

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
        hash_id = make_hash_id(start_dt, end_dt, title, parsed["room"])

        event = {
            "summary": title,
            "location": parsed["room"],
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

    # Si on n'a pas pu cliquer j_n, on tente le bouton “semaine suivante” en fallback
    if not used_tab and week_idx < end_idx:
        if not iter_next_week(pronote):
            break


if __name__ == "__main__":
    try:
        run()
    except Exception as ex:
        try:
            os.makedirs(SCREEN_DIR, exist_ok=True)
        except:
            pass
        print(f"[FATAL] {ex}")
        sys.exit(1)
