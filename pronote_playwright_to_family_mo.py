# pronote_playwright_to_family_mo.py
import os, sys, re, json, hashlib
import datetime as dt
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG =========
# ENT / PRONOTE
ENT_URL     = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL = os.getenv("PRONOTE_URL", "")  # laisser vide pour cliquer la tuile PRONOTE depuis l'ENT

ENT_USER    = os.getenv("PRONOTE_USER", "")  # on réutilise tes secrets existants
ENT_PASS    = os.getenv("PRONOTE_PASS", "")

# Google Calendar
CALENDAR_ID = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX = "[Mo] "
COLOR_ID     = "6"  # 6 = orange

# Fenêtre : semaine en cours + suivante
WEEKS_TO_FETCH = 2

# Debug : mettre HEADFUL=1 pour voir le navigateur sur le runner
HEADFUL = os.getenv("HEADFUL", "0") == "1"

# OAuth Google
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# ========= Google Calendar helpers =========
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
            print(f"[Google OAuth] Erreur: {e}")
            raise
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

def make_event_id(start: datetime, end: datetime, title: str, location: str) -> str:
    base = f"{start.isoformat()}|{end.isoformat()}|{title}|{location}"
    return "mo_" + hashlib.md5(base.encode("utf-8")).hexdigest()

def upsert_event(svc, cal_id: str, ev: Dict[str, Any]) -> str:
    ev_id = ev["id"]
    try:
        svc.events().insert(calendarId=cal_id, body=ev, sendUpdates="none").execute()
        return "created"
    except HttpError as e:
        if getattr(e, "resp", None) is not None and getattr(e.resp, "status", None) == 409:
            svc.events().update(calendarId=cal_id, eventId=ev_id, body=ev, sendUpdates="none").execute()
            return "updated"
        raise

# ========= Parsing helpers =========
HOUR_RE = re.compile(r'(?P<h>\d{1,2})[:hH](?P<m>\d{2})')

def parse_timespan(text: str) -> Optional[tuple]:
    # Matche "08:00 - 09:00" ou "08h00-09h00"
    times = HOUR_RE.findall(text)
    if len(times) >= 2:
        (h1, m1), (h2, m2) = times[0], times[1]
        return (int(h1), int(m1)), (int(h2), int(m2))
    return None

def parse_aria_label(label: str) -> Dict[str, Any]:
    """
    Ex: "08:00 - 09:00 ANGLAIS — Salle 105 — Prof: Dupont"
    """
    d = {"start": None, "end": None, "summary": None, "room": ""}
    lab = " ".join(label.split())
    tspan = parse_timespan(lab)
    if tspan:
        d["start"], d["end"] = tspan

    m_room = re.search(r'(?:Salle|Salles?)\s*([A-Za-z0-9\-_. ]+)', lab, re.IGNORECASE)
    if m_room:
        d["room"] = m_room.group(1).strip()

    summary = lab
    summary = re.sub(r'^\s*\d{1,2}[:hH]\d{2}\s*[–\-]\s*\d{1,2}[:hH]\d{2}\s*', '', summary)
    summary = re.sub(r'(Salle|Salles?).*$', '', summary, flags=re.IGNORECASE)
    summary = re.sub(r'(Prof\.?:.*)$', '', summary, flags=re.IGNORECASE)
    summary = summary.strip(" -–")
    d["summary"] = summary if summary else "Cours"
    return d

def monday_of_week(text_header: str) -> Optional[datetime]:
    # Exemple: "Semaine 37 - 08/09/2025 au 14/09/2025"
    m = re.search(r'(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})', text_header)
    if m:
        d0 = datetime.strptime(m.group(1), "%d/%m/%Y")
        return d0
    return None

def to_datetime(base_monday: Optional[datetime], day_idx: Optional[int], hm: tuple) -> datetime:
    if base_monday is not None and day_idx is not None and 0 <= int(day_idx) <= 6:
        base = base_monday + timedelta(days=int(day_idx))
    else:
        base = datetime.now()
    return base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

# ========= Playwright scraping =========
def login_ent(page):
    page.goto(ENT_URL, wait_until="load")
    page.wait_for_load_state("networkidle")

    # Champs les plus fréquents ENT
    user_selectors = [
        'input[name="email"]',
        'input[name="username"]',
        '#username',
        'input[type="text"][name*="user"]',
    ]
    pass_selectors = [
        'input[type="password"][name="password"]',
        '#password',
        'input[type="password"]',
    ]
    submit_selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Se connecter")',
        'button:has-text("Connexion")',
    ]

    def fill_first(selectors: List[str], value: str) -> bool:
        for sel in selectors:
            loc = page.locator(sel)
            if loc.count() > 0:
                try:
                    loc.first.fill(value)
                    return True
                except:
                    pass
        return False

    if not fill_first(user_selectors, ENT_USER):
        raise RuntimeError("Champ identifiant ENT introuvable. Passe HEADFUL=1 pour ajuster le sélecteur.")
    if not fill_first(pass_selectors, ENT_PASS):
        raise RuntimeError("Champ mot de passe ENT introuvable. Passe HEADFUL=1 pour ajuster le sélecteur.")

    clicked = False
    for sel in submit_selectors:
        loc = page.locator(sel)
        if loc.count() > 0:
            try:
                loc.first.click()
                clicked = True
                break
            except:
                pass
    if not clicked:
        page.keyboard.press("Enter")

    page.wait_for_load_state("networkidle")

def open_pronote(context, page):
    if PRONOTE_URL:
        page.goto(PRONOTE_URL, wait_until="load")
        page.wait_for_load_state("networkidle")
        return page

    # Clique sur la tuile / lien PRONOTE (nouvel onglet ou même onglet)
    with page.expect_popup() as p:
        for sel in ['a:has-text("PRONOTE")', 'a[title*="PRONOTE"]', 'a[href*="pronote"]', 'text=PRONOTE']:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click()
                break
    try:
        pronote_page = p.value
        pronote_page.wait_for_load_state("networkidle")
        return pronote_page
    except PWTimeout:
        page.wait_for_load_state("networkidle")
        return page

def goto_timetable(pronote_page):
    # Essaie de cliquer "Vie scolaire" (selon thèmes)
    for sel in ['text="Vie scolaire"', 'button:has-text("Vie scolaire")', 'a:has-text("Vie scolaire")']:
        if pronote_page.locator(sel).count() > 0:
            try:
                pronote_page.locator(sel).first.click()
                pronote_page.wait_for_timeout(500)
                break
            except:
                pass
    pronote_page.wait_for_load_state("networkidle")
    pronote_page.wait_for_timeout(400)

def extract_week_info(pronote_page) -> Dict[str, Any]:
    header_text = ""
    for sel in ['text=/Semaine .* au .*/', '.titrePeriode', '.zoneSemaines', 'header']:
        try:
            loc = pronote_page.locator(sel)
            if loc.count() > 0:
                header_text = loc.first.inner_text()
                if header_text:
                    break
        except:
            pass

    d0 = monday_of_week(header_text)

    # Collecte des cases : aria-label, title ou fallback texte
    tiles = pronote_page.evaluate("""
    () => {
      const out = [];
      const nodes = new Set();
      const push = (el, label) => {
        if (!el || !label) return;
        const key = el; // ref
        if (!nodes.has(key)) {
          nodes.add(key);
          // heuristique jour
          let dayIndex = null;
          let p = el.parentElement;
          while (p) {
            if (p.hasAttribute && p.hasAttribute('data-dayindex')) {
              dayIndex = parseInt(p.getAttribute('data-dayindex'));
              break;
            }
            p = p.parentElement;
          }
          out.push({ label, dayIndex });
        }
      };

      document.querySelectorAll('[aria-label]').forEach(e => {
        const v = e.getAttribute('aria-label');
        if (v && /\\d{1,2}[:hH]\\d{2}.*\\d{1,2}[:hH]\\d{2}/.test(v)) push(e, v);
      });

      document.querySelectorAll('[title]').forEach(e => {
        const v = e.getAttribute('title');
        if (v && /\\d{1,2}[:hH]\\d{2}.*\\d{1,2}[:hH]\\d{2}/.test(v)) push(e, v);
      });

      // Fallback: texte interne
      document.querySelectorAll('*').forEach(e => {
        const t = (e.innerText || '').trim();
        if (t && /\\d{1,2}[:hH]\\d{2}.*\\d{1,2}[:hH]\\d{2}/.test(t) && t.length < 160) push(e, t);
      });

      return Array.from(out);
    }
    """)
    return {"monday": d0, "tiles": tiles, "header": header_text}

def iter_next_week(pronote_page) -> bool:
    for sel in [
        'button[title*="suivante"]',
        'button[aria-label*="suivante"]',
        'button:has-text("→")',
        'a[title*="suivante"]',
        'a:has-text("Semaine suivante")'
    ]:
        loc = pronote_page.locator(sel)
        if loc.count() > 0:
            try:
                loc.first.click()
                pronote_page.wait_for_load_state("networkidle")
                pronote_page.wait_for_timeout(500)
                return True
            except:
                pass
    return False

# ========= Main =========
def run():
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("Identifiants ENT manquants (PRONOTE_USER / PRONOTE_PASS).")

    svc = get_gcal_service()
    created = updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = context.new_page()

        # 1) ENT login
        login_ent(page)

        # 2) PRONOTE
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # 3) Parcours des semaines
        for w in range(WEEKS_TO_FETCH):
            info = extract_week_info(pronote)
            d0 = info["monday"]
            tiles = info["tiles"] or []
            print(f"Semaine {w+1}: cases trouvées = {len(tiles)}, header='{(info.get('header') or '')[:80]}'")

            for t in tiles:
                label = t.get("label") or ""
                if not label.strip():
                    continue
                parsed = parse_aria_label(label)
                if not parsed["start"] or not parsed["end"]:
                    continue

                start_dt = to_datetime(d0, t.get("dayIndex"), parsed["start"])
                end_dt   = to_datetime(d0, t.get("dayIndex"), parsed["end"])

                # Filtrage fenêtre (évite d'inonder l'agenda si l'UI affiche des semaines très lointaines)
                now = datetime.now()
                if end_dt < (now - timedelta(days=21)) or start_dt > (now + timedelta(days=90)):
                    continue

                title = parsed["summary"].strip() or "Cours"
                title = f"{TITLE_PREFIX}{title}"

                event_id = make_event_id(start_dt, end_dt, title, parsed["room"])
                event = {
                    "id": event_id,
                    "summary": title,
                    "location": parsed["room"],
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                }

                try:
                    action = upsert_event(svc, CALENDAR_ID, event)
                    if action == "created":
                        created += 1
                    else:
                        updated += 1
                except HttpError as e:
                    print(f"[GCAL] Erreur sur {title} ({start_dt}): {e}")

            # Semaine suivante
            if w < WEEKS_TO_FETCH - 1:
                if not iter_next_week(pronote):
                    break

        browser.close()

    print(f"Terminé. créés={created}, maj={updated}")

if __name__ == "__main__":
    try:
        run()
    except Exception as ex:
        print(f"[FATAL] {ex}")
        # En cas d'échec headless, tu peux passer HEADFUL=1 pour voir la scène
        sys.exit(1)
