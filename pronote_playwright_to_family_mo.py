# pronote_playwright_to_family_mo.py
import os, sys, re, json, time, hashlib, datetime as dt
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------- CONFIG ----------
# URLs
ENT_URL      = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL  = os.getenv("PRONOTE_URL", "")  # Optionnel: lien direct PRONOTE (sinon on clique la tuile)

# Identifiants (secrets GitHub / runner)
ENT_USER     = os.getenv("PRONOTE_USER", "")   # on réutilise tes secrets actuels
ENT_PASS     = os.getenv("PRONOTE_PASS", "")

# Calendrier cible (Famille)
CALENDAR_ID  = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")

# Fenêtre de synchro: 2 semaines (Semaine courante + suivante)
WEEKS_TO_FETCH = 2

# Debug: mettre HEADFUL=1 dans les variables si tu veux voir le navigateur
HEADFUL = os.getenv("HEADFUL", "0") == "1"

# Dossiers fichiers OAuth Google
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"

# ---------- Helpers Google Calendar ----------
SCOPES = ["https://www.googleapis.com/auth/calendar"]

def get_gcal_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
            except Exception:
                creds = None
        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

def make_event_id(start: datetime, end: datetime, title: str, location: str) -> str:
    base = f"{start.isoformat()}|{end.isoformat()}|{title}|{location}"
    return "mo_" + hashlib.md5(base.encode("utf-8")).hexdigest()

def upsert_event(svc, cal_id, ev):
    """
    Insertion idempotente par 'id' calculée (si 409 => update).
    """
    ev_id = ev["id"]
    try:
        svc.events().insert(calendarId=cal_id, body=ev, sendUpdates="none").execute()
        return "created"
    except HttpError as e:
        if e.resp is not None and e.resp.status == 409:
            # existe déjà -> update
            svc.events().update(calendarId=cal_id, eventId=ev_id, body=ev, sendUpdates="none").execute()
            return "updated"
        else:
            raise

# ---------- Helpers parsing ----------
HOUR_RE = re.compile(r'(?P<h>\d{1,2})[:hH](?P<m>\d{2})')

def parse_timespan(text: str) -> Optional[tuple]:
    # Ex: "08:00 – 09:00 / 08h00-09h00"
    times = HOUR_RE.findall(text)
    if len(times) >= 2:
        (h1, m1), (h2, m2) = times[0], times[1]
        return (int(h1), int(m1)), (int(h2), int(m2))
    return None

def parse_aria_label(label: str) -> Dict[str, Any]:
    """
    Essaie d’extraire start/end, summary, room à partir d’un aria-label de case.
    On rencontre souvent:
      "08:00 - 09:00 ANGLAIS LV1 — Salle 105 — Prof: Dupont"
    """
    d = {"start": None, "end": None, "summary": None, "room": ""}
    label_clean = " ".join(label.split())
    tspan = parse_timespan(label_clean)
    if tspan:
        d["start"], d["end"] = tspan

    # Room
    m_room = re.search(r'(?:Salle|Salles?)\s*([A-Za-z0-9\-_. ]+)', label_clean, re.IGNORECASE)
    if m_room:
        d["room"] = m_room.group(1).strip()

    # Summary: on tente de retirer les heures et les mots clefs
    summary = label_clean
    summary = re.sub(r'^\s*\d{1,2}[:hH]\d{2}\s*[–\-]\s*\d{1,2}[:hH]\d{2}\s*', '', summary)
    summary = re.sub(r'(Salle|Salles?).*$', '', summary, flags=re.IGNORECASE)
    summary = re.sub(r'(Prof\.?:.*)$', '', summary, flags=re.IGNORECASE)
    summary = summary.strip(" -–")
    d["summary"] = summary if summary else "Cours"
    return d

def monday_of_week(text_header: str) -> Optional[datetime]:
    """
    Sur beaucoup d'instances, la zone haute affiche:
      "Semaine 37 - 08/09/2025 au 14/09/2025"
    On essaye de lire la date de début.
    """
    m = re.search(r'(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})', text_header)
    if m:
        d0 = datetime.strptime(m.group(1), "%d/%m/%Y")
        return d0
    return None

# ---------- Playwright scraping ----------
def login_ent(page):
    page.goto(ENT_URL, wait_until="load")
    # Quelques sélecteurs typiques ENT (à adapter si besoin)
    user_selectors = [
        'input[name="email"]', 'input[name="username"]', '#username', 'input[type="text"]'
    ]
    pass_selectors = [
        'input[type="password"][name="password"]', 'input[type="password"]', '#password'
    ]
    submit_selectors = [
        'button[type="submit"]', 'input[type="submit"]', 'button:has-text("Se connecter")', 'button:has-text("Connexion")'
    ]

    def fill_first(selector_list, value):
        for sel in selector_list:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.fill(value)
                return True
        return False

    if not fill_first(user_selectors, ENT_USER):
        raise RuntimeError("Sélecteur identifiant ENT introuvable. Lance en HEADFUL et adapte les sélecteurs.")
    if not fill_first(pass_selectors, ENT_PASS):
        raise RuntimeError("Sélecteur mot de passe ENT introuvable. Lance en HEADFUL et adapte les sélecteurs.")

    # Clique sur un des boutons submit
    clicked = False
    for sel in submit_selectors:
        loc = page.locator(sel)
        if loc.count() > 0:
            loc.first.click()
            clicked = True
            break
    if not clicked:
        # Tente Enter
        page.keyboard.press("Enter")

    # Attendre l'arrivée sur la page principale ENT
    page.wait_for_load_state("networkidle")

def open_pronote(context, page):
    if PRONOTE_URL:
        page.goto(PRONOTE_URL)
        page.wait_for_load_state("networkidle")
        return page

    # Sinon, cliquer la tuile/lien "PRONOTE"
    # Beaucoup d’ENT ont un lien visible "PRONOTE"
    # On gère le popup éventuel
    with page.expect_popup() as p:
        # Essaye plusieurs variantes
        for sel in ['a:has-text("PRONOTE")', 'a[title*="PRONOTE"]', 'a[href*="pronote"]', 'text=PRONOTE']:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click()
                break
    try:
        pronote_page = p.value
        pronote_page.wait_for_load_state("networkidle")
        return pronote_page
    except PWTimeout:
        # Parfois c'est le même onglet
        page.wait_for_load_state("networkidle")
        return page

def goto_timetable(pronote_page):
    """
    Cliquer sur 'Vie scolaire' puis s’assurer qu’on est sur la grille d’emploi du temps.
    Les intitulés diffèrent parfois; on couvre quelques variantes.
    """
    # Onglet "Vie scolaire"
    for sel in ['text="Vie scolaire"', 'button:has-text("Vie scolaire")', 'a:has-text("Vie scolaire")']:
        if pronote_page.locator(sel).count() > 0:
            pronote_page.locator(sel).first.click()
            break

    # Attendre la grille ou la mention de semaine
    pronote_page.wait_for_timeout(800)  # petit délai UI
    # Si tu as un bouton « Semaine suivante » / « → » :
    # on essaye de le trouver pour les itérations
    return

def extract_week_info(pronote_page) -> Dict[str, Any]:
    """Retourne dict avec:
        monday (datetime) si détectable,
        tiles (liste d'objets {label, day_index})"""
    header_text = ""
    try:
        # Bandeau semaine (varie selon thèmes)
        for sel in ['text=/Semaine .* au .*/', '.titrePeriode', '.zoneSemaines', 'header']:
            if pronote_page.locator(sel).count() > 0:
                header_text = pronote_page.locator(sel).first.inner_text()
                if header_text:
                    break
    except:
        pass

    monday = monday_of_week(header_text)  # peut être None si introuvable

    # On récupère les cases par aria-label (souvent présent sur chaque cours)
    # On essaie aussi de récupérer l'index du jour via la colonne parent (si dispo)
    tiles = pronote_page.evaluate("""
    () => {
      const out = [];
      const els = Array.from(document.querySelectorAll('[aria-label*=":"]'));
      for (const e of els) {
        const label = e.getAttribute('aria-label') || e.innerText || '';
        // dayIndex heuristique: chercher le parent colonnaire
        let dayIndex = null;
        let p = e.parentElement;
        while (p) {
          if (p.hasAttribute('data-dayindex')) { dayIndex = parseInt(p.getAttribute('data-dayindex')); break; }
          p = p.parentElement;
        }
        out.push({ label, dayIndex });
      }
      return out;
    }
    """)
    return {"monday": monday, "tiles": tiles, "header": header_text}

def iter_next_week(pronote_page) -> bool:
    """
    Clique 'semaine suivante' si trouvé.
    Retourne True si le clic a été fait.
    """
    for sel in [
        'button[title*="suivante"]',
        'button[aria-label*="suivante"]',
        'button:has-text("→")',
        'a[title*="suivante"]',
    ]:
        if pronote_page.locator(sel).count() > 0:
            pronote_page.locator(sel).first.click()
            pronote_page.wait_for_load_state("networkidle")
            pronote_page.wait_for_timeout(500)
            return True
    return False

def to_datetime(d0: Optional[datetime], day_idx: Optional[int], hm: tuple) -> datetime:
    """
    Si on a monday (d0) + day_idx on calcule la date; sinon on prend la date du jour (approx).
    """
    if d0 and day_idx is not None and 0 <= day_idx <= 6:
        base = d0 + timedelta(days=int(day_idx))
    else:
        # fallback: semaine courante
        today = datetime.now()
        base = today  # approximation
    return base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

def run():
    svc = get_gcal_service()
    created = updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = context.new_page()

        # 1) ENT
        login_ent(page)

        # 2) PRONOTE
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # 3) Parcourir n semaines
        for w in range(WEEKS_TO_FETCH):
            info = extract_week_info(pronote)
            d0 = info["monday"]  # peut être None -> fallback date courante
            tiles = info["tiles"]

            # Parse de chaque case
            for t in tiles:
                label = t.get("label") or ""
                if not label.strip():
                    continue
                parsed = parse_aria_label(label)
                if not parsed["start"] or not parsed["end"]:
                    # pas d'heures détectées -> on ignore
                    continue

                start_dt = to_datetime(d0, t.get("dayIndex"), parsed["start"])
                end_dt   = to_datetime(d0, t.get("dayIndex"), parsed["end"])

                # Borne dans +/- 30 jours pour éviter des insertions inutiles
                now = datetime.now()
                if end_dt < (now - timedelta(days=14)) or start_dt > (now + timedelta(days=60)):
                    continue

                title = parsed["summary"].strip()
                if not title:
                    title = "Cours"
                # Préfixe [Mo] comme demandé
                title = f"[Mo] {title}"

                event_id = make_event_id(start_dt, end_dt, title, parsed["room"])
                event = {
                    "id": event_id,
                    "summary": title,
                    "location": parsed["room"],
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": "5",
                }
                try:
                    action = upsert_event(svc, CALENDAR_ID, event)
                    if a
