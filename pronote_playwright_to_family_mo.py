# pronote_playwright_to_family_mo.py
# -*- coding: utf-8 -*-

import os
import sys
import re
import hashlib
import unicodedata
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ========= CONFIG (via env) =========
ENT_URL     = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL = os.getenv("PRONOTE_URL", "")   # si vide, on clique la tuile PRONOTE depuis l’ENT
ENT_USER    = os.getenv("PRONOTE_USER", "")
ENT_PASS    = os.getenv("PRONOTE_PASS", "")

# Sélecteurs optionnels pour arriver à l’EDT (tes CSS copiés depuis l’inspecteur)
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()   # ex: "parent.html" pour n’agir que dans cette frame

# Onglets semaine (si tu veux cliquer j_1, j_2, …) — non indispensables maintenant
WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()      # ex: #GInterface\.Instances\[2\]\.Instances\[0\]_j_{n}
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))         # non utilisé si on déroule avec “Semaine suivante”

# Boucle semaines (courante + suivantes)
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))           # ~1 mois
WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

# Google Calendar
CALENDAR_ID  = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX = "[Mo] "
COLOR_ID     = "6"      # 6 = orange

HEADFUL      = os.getenv("HEADFUL", "0") == "1"

# OAuth files
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts & captures
TIMEOUT_MS = 120_000
SCREEN_DIR = "screenshots"


# ========= Google Calendar helpers =========
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


# ========= Utils: texte/horaires =========
HOUR_RE = re.compile(r'(\d{1,2})\s*(?:h|heures|:)\s*(\d{2})', re.IGNORECASE)

MONTHS_FR = {
    "janvier": 1, "janv": 1, "jan.": 1,
    "février": 2, "fevrier": 2, "févr": 2, "fevr": 2, "fév.": 2, "fev.": 2,
    "mars": 3,
    "avril": 4, "avr": 4, "avr.": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7, "juil": 7, "juil.": 7,
    "août": 8, "aout": 8,
    "septembre": 9, "sept": 9, "sept.": 9,
    "octobre": 10, "oct": 10, "oct.": 10,
    "novembre": 11, "nov": 11, "nov.": 11,
    "décembre": 12, "decembre": 12, "déc": 12, "dec": 12, "déc.": 12, "dec.": 12,
}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def parse_french_date(text: str, header_year_hint: Optional[int]) -> Optional[datetime]:
    """
    Cherche '8 septembre' (ou variantes) dans text, retourne une date (à minuit).
    Si aucune année explicite, on utilise header_year_hint ou l'année courante.
    """
    t = _norm(text)
    m = re.search(r'(\d{1,2})\s+([a-zéû\.]+)', t)
    if not m:
        return None
    day = int(m.group(1))
    mon_str = m.group(2).strip(".")
    # simplifie accents/abréviations
    mon = MONTHS_FR.get(mon_str, None)
    if not mon:
        # tente encore sans accents
        mon = MONTHS_FR.get(mon_str.replace("é", "e").replace("û", "u"), None)
    if not mon:
        return None

    year = header_year_hint or datetime.now().year
    try:
        return datetime(year, mon, day)
    except ValueError:
        return None


def parse_aria_course(aria: str, week_header: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    aria ex: "Cours du 8 septembre de 9 heures 05 à 10 heures 00"
    week_header peut contenir "du 08/09/2025 au 12/09/2025" pour déduire l'année.
    """
    year_hint = None
    mh = re.search(r'(\d{2})/(\d{2})/(\d{4})', week_header)
    if mh:
        year_hint = int(mh.group(3))

    # date
    base_day = parse_french_date(aria or "", year_hint)
    if not base_day:
        return None, None

    # heures
    times = HOUR_RE.findall(aria or "")
    if len(times) < 2:
        return None, None

    (h1, m1), (h2, m2) = times[0], times[1]
    start_dt = base_day.replace(hour=int(h1), minute=int(m1), second=0, microsecond=0)
    end_dt   = base_day.replace(hour=int(h2), minute=int(m2), second=0, microsecond=0)
    if end_dt <= start_dt:
        end_dt += timedelta(hours=1)  # garde-fou
    return start_dt, end_dt


def guess_room(text: str) -> str:
    """
    Petites heuristiques pour trouver une 'salle' dans le bloc texte.
    Exemples vus: 'S01', '110', '004', parfois précédés de 'Salle'.
    """
    t = _norm(text)
    # 's01', 's11', 's-01'...
    m = re.search(r'\b(salle|s)\s*[-_]?\s*([a-z0-9]{2,5})\b', t, re.IGNORECASE)
    if m:
        return m.group(2).upper()
    # 3 chiffres comme '110', '004'
    m = re.search(r'\b\d{3}\b', t)
    if m:
        return m.group(0)
    return ""


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
        "Tout accepter", "Accepter tout", "J'accepte", "Accepter",
        "OK", "Continuer", "J’ai compris", "J'ai compris"
    ]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_in_frames(page, sels)


def _frame_has_timetable_js():
    # Détection généreuse: texte “Emploi du temps” + traces de semaines / horaires.
    return r"""
      () => {
        const txt = (document.body.innerText || '').replace(/\s+/g,' ');
        const hasTitle = /Emploi du temps/i.test(txt) || /Planning|Agenda/i.test(txt);
        const hasWeek  = /(Semaine|du\s+\d{1,2}\/\d{1,2}\/\d{4}\s+au\s+\d{1,2}\/\d{1,2}\/\d{4})/i.test(txt);
        const hasTimes = /\d{1,2}\s*(h|heures|:)\s*\d{2}/i.test(txt);
        return (hasTitle && (hasTimes || hasWeek)) || (hasWeek && hasTimes);
      }
    """


def wait_timetable_any_frame(page, timeout_ms=120_000):
    deadline = time.time() + timeout_ms / 1000.0
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
                    }""",
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
                page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                if screenshot_tag:
                    try:
                        page.screenshot(path=f"{SCREEN_DIR}/08-clicked-{screenshot_tag}.png", full_page=True)
                    except:
                        pass
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
        'a:has-text("Se connecter")', 'a:has-text("Connexion")',
        'button:has-text("Se connecter")', 'button:has-text("Connexion")',
        'a[href*="login"]', 'a[href*="auth"]'
    ])
    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    page.screenshot(path=f"{SCREEN_DIR}/02-ent-after-click-login.png", full_page=True)

    user_candidates = [
        'input[name="email"]', 'input[name="username"]', '#username',
        'input[type="text"][name*="user"]', 'input[type="text"]', 'input[type="email"]',
        'input#email', 'input[name="login"]', 'input[name="j_username"]'
    ]
    pass_candidates = [
        'input[type="password"][name="password"]', '#password', 'input[type="password"]', 'input[name="j_password"]'
    ]
    submit_candidates = [
        'button[type="submit"]', 'input[type="submit"]',
        'button:has-text("Se connecter")', 'button:has-text("Connexion")', 'button:has-text("Valider")'
    ]

    user_loc = first_locator_in_frames(page, user_candidates)
    pass_loc = first_locator_in_frames(page, pass_candidates)
    if not user_loc or not pass_loc:
        click_first_in_frames(page, [
            'button:has-text("Identifiant")', 'a:has-text("Identifiant")',
            'button:has-text("Compte")', 'a:has-text("Compte")', 'a:has-text("ENT")'
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
            'a:has-text("PRONOTE")', 'a[title*="PRONOTE"]', 'a[href*="pronote"]', 'text=PRONOTE'
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

    # Ton chemin “Vie scolaire → Emploi du temps” si nécessaire
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

    # Déjà dessus ?
    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=10_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-already-here.png", full_page=True)
        return fr
    except TimeoutError:
        pass

    # Heuristiques (menu texte)
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

    # Dernier essai
    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=15_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-ready-fallback.png", full_page=True)
        return fr
    except TimeoutError:
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-NOT-found.png", full_page=True)
        raise RuntimeError("Impossible d’atteindre l’Emploi du temps.")


def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        click_text_anywhere(page, ["Tout voir", "Voir tout", "Tout afficher"])
        page.wait_for_timeout(400)


def iter_next_week(pronote_page) -> bool:
    if click_first_in_frames(pronote_page, [
        'button[title*="suivante"]', 'button[aria-label*="suivante"]', 'button:has-text("→")',
        'a[title*="suivante"]', 'a:has-text("Semaine suivante")'
    ]):
        accept_cookies_any(pronote_page)
        wait_for_timetable_ready(pronote_page)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/09-pronote-next-week.png", full_page=True)
        return True
    return False


# ========= Extraction EDT =========
def extract_week_info(pronote_page) -> Dict[str, Any]:
    """
    Ne lit que dans la grille EDT : items 'cours' avec aria-label “Cours du … de … à …”.
    On rapatrie l’aria + un résumé texte depuis la cellule principale.
    """
    # header semaine (pour l'année)
    header_text = ""
    for sel in ['text=/du \\d{2}\\/\\d{2}\\/\\d{4} au \\d{2}\\/\\d{2}\\/\\d{4}/i', '.titrePeriode', '.zoneSemaines', 'header']:
        loc = first_locator_in_frames(pronote_page, [sel])
        if loc:
            try:
                header_text = loc.inner_text().strip()
                if header_text:
                    break
            except:
                pass

    items = pronote_page.evaluate(r"""
    () => {
      const out = [];
      // on cible les conteneurs "cours" (grille EDT)
      const listItems = Array.from(document.querySelectorAll('div[id*="_cours_"][role="listitem"]'));
      for (const li of listItems) {
        const grp = li.querySelector('[role="group"][aria-label]');
        if (!grp) continue;
        const aria = grp.getAttribute('aria-label') || '';

        // texte principal dans la 1ère cellule (souvent *_cont0)
        let text = '';
        const td = grp.querySelector('td[id$="_cont0"]');
        if (td && td.innerText) text = td.innerText.trim();
        else if (grp.innerText) text = grp.innerText.trim();

        // on filtre: seulement si l’aria contient 2 horaires (ex: "de 9 heures 05 à 10 heures 00")
        const rxTime = /\d{1,2}\s*(?:h|heures|:)\s*\d{2}.*?\d{1,2}\s*(?:h|heures|:)\s*\d{2}/i;
        if (!rxTime.test(aria)) continue;

        out.push({ aria, text });
      }
      return out;
    }
    """)

    return {"header": header_text or "", "items": items}


# ========= Main =========
def run():
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("Identifiants ENT manquants: PRONOTE_USER / PRONOTE_PASS.")

    svc = get_gcal_service()
    created = updated = 0
    seen_hashes = set()  # évite doublons au sein d’un même run

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = context.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        # ENT → PRONOTE → Emploi du temps
        login_ent(page)
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # --- Semaine courante + suivantes ---
        for w in range(WEEKS_TO_FETCH):
            if w > 0:
                if not iter_next_week(pronote):
                    break

            accept_cookies_any(pronote)
            ensure_all_visible(pronote)

            info = extract_week_info(pronote)
            hdr = info.get("header", "")
            items = info.get("items", [])
            print(f"Semaine {w+1}: {len(items)} cases, header='{hdr}'")

            # capture d’écran de la semaine
            try:
                pronote.screenshot(path=f"{SCREEN_DIR}/week-{w+1}.png", full_page=True)
            except:
                pass

            for it in items:
                aria = it.get("aria", "")
                block_text = it.get("text", "")

                start_dt, end_dt = parse_aria_course(aria, hdr)
                if not start_dt or not end_dt:
                    continue

                # limite temporelle : -21j à +90j
                now = datetime.now()
                if end_dt < (now - timedelta(days=21)) or start_dt > (now + timedelta(days=90)):
                    continue

                summary = (block_text.splitlines()[0].strip() if block_text else "Cours")
                room = guess_room(block_text)

                title = f"{TITLE_PREFIX}{summary}"
                h = make_hash_id(start_dt, end_dt, title, room)
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)

                event = {
                    "summary": title,
                    "location": room,
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"mo_hash": h, "source": "pronote_playwright"}},
                }

                try:
                    action = upsert_event_by_hash(svc, CALENDAR_ID, h, event)
                    if action == "created":
                        created += 1
                    else:
                        updated += 1
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
        except:
            pass
        print(f"[FATAL] {ex}")
        sys.exit(1)
