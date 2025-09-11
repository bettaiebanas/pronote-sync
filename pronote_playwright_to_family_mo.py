# -*- coding: utf-8 -*-
# pronote_playwright_to_family_mo.py
#
# ENT -> PRONOTE (Playwright) -> extraction Emploi du temps -> Google Calendar (Famille)
#
# Points-clés :
# - Navigation ENT -> PRONOTE, puis vers "Emploi du temps"
# - Clic direct sur tes onglets de semaines : WEEK_TAB_TEMPLATE = "#GInterface\\.Instances\\[2\\]\\.Instances\\[0\\]_j_{n}"
# - Extraction PRONOTE fiable : on lit les groupes de cours (role="group", aria-label "Cours du ... de ... à ...")
#   + Lecture du texte matière/prof/groupe/salle dans td[id*="_cont0"/"_cont1"] du même listitem
# - Fusion des séances contiguës (pour éviter les doublons “découpés”)
# - Upsert idempotent sur Google Calendar via extendedProperties.private.mo_hash
#
# Logs : on affiche ce qu’on fait (calendrier trouvé, nb cours par semaine, nb avant/après fusion, etc.)

import os, sys, re, unicodedata, hashlib, time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ====================== CONFIG ======================
ENT_URL       = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL   = os.getenv("PRONOTE_URL", "")   # si vide : on clique la tuile depuis l’ENT
ENT_USER      = os.getenv("PRONOTE_USER", "")
ENT_PASS      = os.getenv("PRONOTE_PASS", "")

# Accès direct à l’EDT (chemin manuel “Vie scolaire -> Emploi du temps”)
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()

# Onglets “Semaine” (j_1, j_2, …)
WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()  # ex: "#GInterface\\.Instances\\[2\\]\\.Instances\\[0\\]_j_{n}"
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))

WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

# Google Calendar
CALENDAR_ID   = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX  = "[Mo] "
COLOR_ID      = "6"   # orange
HEADFUL       = os.getenv("HEADFUL", "0") == "1"

# OAuth
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]  # attention : scope exact

# Timeouts & captures
TIMEOUT_MS = 120_000
SCREEN_DIR = "screenshots"


# ====================== GOOGLE CALENDAR ======================
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

    svc = build("calendar", "v3", credentials=creds)

    # Log de contrôle : on vérifie que CALENDAR_ID existe bien côté compte
    try:
        resp = svc.calendarList().list().execute()
        items = resp.get("items", [])
        ids = {it.get("id") for it in items}
        print(f"[DBG] CalendarList loaded: {len(items)} calendars. CALENDAR_ID present? {CALENDAR_ID in ids}")
    except Exception as e:
        print(f"[DBG] CalendarList check failed: {e}")

    return svc


def make_hash_id(start: datetime, end: datetime, title: str, location: str) -> str:
    """Hash stable pour dédup (stocké en extendedProperties.private.mo_hash)."""
    base = f"{start.isoformat()}|{end.isoformat()}|{_norm(title)}|{_norm(location)}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def find_event_by_hash(svc, cal_id: str, h: str) -> Optional[Dict[str, Any]]:
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
        print(f"[GCAL:list] {e}")
        return None


def upsert_event_by_hash(svc, cal_id: str, h: str, body: Dict[str, Any]) -> str:
    """Idempotent : si mo_hash existe -> update, sinon insert."""
    existing = find_event_by_hash(svc, cal_id, h)
    try:
        if existing:
            svc.events().update(calendarId=cal_id, eventId=existing["id"], body=body, sendUpdates="none").execute()
            return "updated"
        else:
            svc.events().insert(calendarId=cal_id, body=body, sendUpdates="none").execute()
            return "created"
    except HttpError as e:
        print(f"[GCAL:upsert] {e}")
        return "error"


# ====================== OUTILS PRONOTE ======================
def first_locator_in_frames(page, selectors: List[str]):
    for fr in page.frames:
        for sel in selectors:
            try:
                loc = fr.locator(sel)
                if loc.count() > 0:
                    return loc.first
            except:
                pass
    return None


def click_first_in_frames(page, selectors: List[str]) -> bool:
    loc = first_locator_in_frames(page, selectors)
    if not loc:
        return False
    try:
        loc.click()
        return True
    except:
        return False


def accept_cookies_any(page):
    texts = [
        "Tout accepter", "Accepter tout", "J'accepte", "Accepter",
        "OK", "Continuer", "J’ai compris", "J'ai compris"
    ]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    click_first_in_frames(page, sels)


def _frame_has_timetable_js():
    # Très permissif : “Emploi du temps” + horaires ou période
    return r"""
      () => {
        const txt = (document.body.innerText || '').replace(/\s+/g, ' ');
        const hasTitle = /Emploi du temps/i.test(txt) || /Planning|Agenda/i.test(txt);
        const hasWeek  = /(Semaine|du\s+\d{1,2}\/\d{1,2}\/\d{4}\s+au\s+\d{1,2}\/\d{1,2}\/\d{4})/i.test(txt);
        const hasTimes = /\d{1,2}\s*[h:]\s*\d{2}/i.test(txt);
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
    raise TimeoutError("Emploi du temps introuvable dans les frames")


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

    # champs identifiants
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
            'button:has-text("Compte")', 'a:has-text("ENT")'
        ])
        page.wait_for_load_state("domcontentloaded")
        accept_cookies_any(page)
        user_loc = first_locator_in_frames(page, user_candidates)
        pass_loc = first_locator_in_frames(page, pass_candidates)

    if not user_loc or not pass_loc:
        page.screenshot(path=f"{SCREEN_DIR}/03-ent-no-fields.png", full_page=True)
        raise RuntimeError("Champ identifiant ENT introuvable. Passe HEADFUL=1 et ajuste.")

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
    pronote_page.set_default_timeout(TIMEOUT_MS)
    accept_cookies_any(pronote_page)

    # Chemin manuel si fourni (Vie scolaire -> Emploi du temps)
    if TIMETABLE_PRE_SELECTOR:
        click_css_in_frames(pronote_page, TIMETABLE_PRE_SELECTOR, TIMETABLE_FRAME, "pre-selector")
    if TIMETABLE_SELECTOR:
        if click_css_in_frames(pronote_page, TIMETABLE_SELECTOR, TIMETABLE_FRAME, "timetable-selector"):
            accept_cookies_any(pronote_page)
            try:
                fr = wait_timetable_any_frame(pronote_page, timeout_ms=30_000)
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-custom-selector.png", full_page=True)
                return fr
            except TimeoutError:
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-custom-timeout.png", full_page=True)

    # Déjà dessus ?
    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=10_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-already-here.png", full_page=True)
        return fr
    except TimeoutError:
        pass

    # Heuristiques (textes)
    attempts = [
        ["Emploi du temps", "Mon emploi du temps", "Emplois du temps"],
        ["Planning", "Agenda"],
        ["Vie scolaire", "Emploi du temps"],
    ]
    for i, pats in enumerate(attempts, 1):
        for pat in pats:
            if click_first_in_frames(pronote_page, [
                f'role=link[name=/{pat}/i]', f'role=button[name=/{pat}/i]',
                f'text=/{pat}/i'
            ]):
                accept_cookies_any(pronote_page)
                try:
                    fr = wait_timetable_any_frame(pronote_page, timeout_ms=30_000)
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-ready-{i}-{pat}.png", full_page=True)
                    return fr
                except TimeoutError:
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-not-ready-{i}-{pat}.png", full_page=True)
        pronote_page.wait_for_timeout(600)

    # Dernière chance
    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=15_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-ready-fallback.png", full_page=True)
        return fr
    except TimeoutError:
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-NOT-found.png", full_page=True)
        raise RuntimeError("Impossible d’atteindre l’Emploi du temps.")


def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        click_first_in_frames(page, [
            'button:has-text("Tout voir")', 'text=/Tout voir/i', 'button:has-text("Voir tout")'
        ])
        page.wait_for_timeout(400)


def goto_week_by_index(page, n: int) -> bool:
    """Clique l’onglet semaine j_n si WEEK_TAB_TEMPLATE est fourni."""
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)
    ok = click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}")
    if ok:
        try:
            wait_timetable_any_frame(page, timeout_ms=12_000)
        except TimeoutError:
            pass
    return ok


# =============== EXTRACTION PRONOTE : COURS (JS) ===============
def extract_courses_from_aria(page, header_monday: Optional[datetime]) -> List[Dict[str, Any]]:
    """
    Extraction fiable :
    - repère chaque cours via le group [role="group"][aria-label*="Cours du … de … à …"]
    - remonte au listitem parent pour lire le contenu (matière, salle, etc.) dans td[id*="_cont0"/"_cont1"]
    - utilise l’année depuis le header (monday) si fournie, sinon cherche une date dd/mm/yyyy dans l’entête
    """
    hd = header_monday.isoformat()[:10] if header_monday else ""
    results = page.evaluate(
        r"""
        (headerMondayISO) => {
          const mois = {
            "janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,"mai":5,"juin":6,
            "juillet":7,"août":8,"aout":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,"decembre":12
          };
          const out = [];

          // Tenter de deviner l'année depuis l'entête visible
          let yearGuess = null;
          const headerCandidates = Array.from(document.querySelectorAll('.titrePeriode, .zoneSemaines, header, body'));
          for (const h of headerCandidates) {
            const txt = (h.innerText || "").replace(/\s+/g,' ');
            const m = txt.match(/(\d{2})\/(\d{2})\/(\d{4})/);
            if (m) { yearGuess = parseInt(m[3],10); break; }
          }
          if (!yearGuess && headerMondayISO) {
            yearGuess = parseInt(headerMondayISO.slice(0,4),10);
          }
          if (!yearGuess) {
            yearGuess = new Date().getFullYear();
          }

          // Sélection : tous les groupes de cours
          const groups = Array.from(document.querySelectorAll('[role="group"][aria-label*="Cours"]'));
          for (const g of groups) {
            const label = g.getAttribute('aria-label') || '';
            // "Cours du 8 septembre de 9 heures 05 à 10 heures 00"
            const mDate  = label.match(/du\s+(\d{1,2})\s+([a-zéû]+)\s+de/i);
            const mTimes = label.match(/de\s+(\d{1,2})\s*heures?\s*(\d{2})\s*à\s+(\d{1,2})\s*heures?\s*(\d{2})/i);
            if (!mDate || !mTimes) continue;

            const d = parseInt(mDate[1],10);
            const moisName = (mDate[2] || '').toLowerCase();
            const mm = mois[moisName] || null;
            if (!mm) continue;

            const sh = parseInt(mTimes[1],10), sm = parseInt(mTimes[2],10);
            const eh = parseInt(mTimes[3],10), em = parseInt(mTimes[4],10);

            // Sujet/salle : remonter au listitem parent et lire *_cont0|1
            let li = g.closest('div[id*="_cours_"]');
            let subject = "", room = "";
            if (li) {
              // cont0 contient souvent Matière / Prof / Groupe / Salle
              const cont = li.querySelector('td[id*="_cont0"]') || li.querySelector('td[id*="_cont1"]') || li;
              const lines = Array.from(cont.querySelectorAll('.NoWrap, div, span')).map(x => (x.innerText||'').trim()).filter(Boolean);
              if (!lines.length) {
                const t = (cont.innerText || '').trim();
                if (t) lines.push(...t.split('\n').map(s => s.trim()).filter(Boolean));
              }
              if (lines.length) {
                subject = lines[0];  // première ligne = matière
                // dernière ligne qui ressemble à une salle (alphanum court)
                for (let i = lines.length - 1; i >= 0; i--) {
                  const L = lines[i];
                  if (/^[A-Za-z0-9\-_.]{2,10}$/.test(L)) { room = L; break; }
                }
              }
            }

            // Construire ISO
            const yyyy = yearGuess;
            const pad = (n)=> (n<10?('0'+n):(''+n));
            const date = `${yyyy}-${pad(mm)}-${pad(d)}`;
            const startISO = `${date}T${pad(sh)}:${pad(sm)}:00`;
            const endISO   = `${date}T${pad(eh)}:${pad(em)}:00`;

            out.push({ startISO, endISO, subject, room });
          }

          return out;
        }
        """,
        hd
    )

    # Convertir en datetime + normaliser
    out_py: List[Dict[str, Any]] = []
    for it in results or []:
        try:
            s = datetime.fromisoformat(it["startISO"])
            e = datetime.fromisoformat(it["endISO"])
            subj = (it.get("subject") or "").strip()
            room = (it.get("room") or "").strip()
            if e > s:
                out_py.append({"start": s, "end": e, "subject": subj, "room": room, "date": s.date()})
        except Exception:
            pass
    return out_py


# =============== FUSION DES SÉANCES CONTIGUËS ===============
def coalesce(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fusionner les fragments successifs du même cours (même jour, même sujet/salle)
    si l’écart entre fin & début <= 10 minutes (ou chevauchement).
    """
    if not entries:
        return []
    entries = sorted(entries, key=lambda x: (x["date"], _norm(x["subject"]), _norm(x.get("room","")), x["start"]))
    merged: List[Dict[str, Any]] = []
    for e in entries:
        if not merged:
            merged.append(e); continue
        last = merged[-1]
        same = (last["date"] == e["date"]
                and _norm(last["subject"]) == _norm(e["subject"])
                and _norm(last.get("room","")) == _norm(e.get("room","")))
        if not same:
            merged.append(e); continue
        # contigu/chevauchement ?
        gap = (e["start"] - last["end"]).total_seconds()
        if gap <= 600:  # 10 min
            last["end"] = max(last["end"], e["end"])
        else:
            merged.append(e)
    return merged


# ====================== MAIN ======================
def run():
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("Identifiants ENT manquants : PRONOTE_USER / PRONOTE_PASS.")

    svc = get_gcal_service()
    created = updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = context.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        # ENT -> PRONOTE -> Emploi du temps
        login_ent(page)
        pronote = open_pronote(context, page)
        goto_timetable(pronote)

        # Fenêtre de semaines
        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1
        print(f"[CFG] Weeks: {start_idx}..{end_idx}")

        # “Ancre” lundi (année de référence)
        anchor_monday: Optional[datetime] = None
        anchor_week_idx: Optional[int] = None

        for week_idx in range(start_idx, end_idx + 1):
            used_tab = goto_week_by_index(pronote, week_idx)
            accept_cookies_any(pronote)
            ensure_all_visible(pronote)
            pronote.screenshot(path=f"{SCREEN_DIR}/08-week-{week_idx}.png", full_page=True)

            # Deviner l’année depuis l’entête (dd/mm/yyyy)
            if anchor_monday is None:
                try:
                    fr = wait_timetable_any_frame(pronote, timeout_ms=8_000)
                    hdr_txt = ""
                    for sel in ['.titrePeriode', '.zoneSemaines', 'header', 'text=/Semaine .* au .*/']:
                        loc = fr.locator(sel)
                        if loc.count() > 0:
                            hdr_txt = (loc.first.inner_text() or "").strip()
                            if hdr_txt:
                                break
                    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', hdr_txt or "")
                    if m:
                        dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                        anchor_monday = datetime(yyyy, mm, dd)
                        anchor_week_idx = week_idx
                        print(f"[DBG] anchor monday={anchor_monday.date()} at tab {anchor_week_idx}")
                except Exception:
                    pass

            # Calcul monday “guess” pour cette tab (utile si aria-label sans année)
            monday_guess: Optional[datetime] = None
            if anchor_monday is not None and anchor_week_idx is not None:
                monday_guess = anchor_monday + timedelta(days=7 * (week_idx - anchor_week_idx))

            # ===== Extraction =====
            tiles = extract_courses_from_aria(pronote, header_monday=monday_guess)
            print(f"Semaine {week_idx}: {len(tiles)} cours, header='{'' if not anchor_monday else anchor_monday.strftime('%d/%m/%Y')}'")

            # Construction des events
            entries: List[Dict[str, Any]] = []
            now = datetime.now()
            for t in tiles:
                start_dt, end_dt = t["start"], t["end"]
                subj = (t.get("subject") or "Cours").strip()
                room = (t.get("room") or "").strip()

                # Fenêtre raisonnable (anti-artefacts)
                if end_dt < (now - timedelta(days=60)) or start_dt > (now + timedelta(days=180)):
                    continue

                title = f"{TITLE_PREFIX}{subj}"
                entries.append({
                    "subject": subj, "room": room, "title": title,
                    "start": start_dt, "end": end_dt, "date": start_dt.date()
                })

            print(f"[DBG]   entries construits: {len(entries)}")
            entries = coalesce(entries)
            print(f"[DBG]   après fusion: {len(entries)}")

            # Upsert Google Calendar
            for e in entries:
                h = make_hash_id(e["start"], e["end"], e["title"], e["room"])
                body = {
                    "summary": e["title"],
                    "location": e["room"],
                    "start": {"dateTime": e["start"].isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": e["end"].isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"mo_hash": h, "source": "pronote_playwright"}},
                }
                action = upsert_event_by_hash(svc, CALENDAR_ID, h, body)
                if action == "created": created += 1
                elif action == "updated": updated += 1

            # Fallback navigation “Semaine suivante”
            if not used_tab and week_idx < end_idx:
                if not click_first_in_frames(pronote, [
                    'button[title*="suivante"]', 'button[aria-label*="suivante"]',
                    'a[title*="suivante"]', 'a:has-text("Semaine suivante")'
                ]):
                    break
                accept_cookies_any(pronote)
                try:
                    wait_timetable_any_frame(pronote, timeout_ms=12_000)
                except TimeoutError:
                    pass

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
