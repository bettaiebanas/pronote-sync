# pronote_playwright_to_family_mo.py
import os, sys, re, hashlib, time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG =========
ENT_URL       = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL   = os.getenv("PRONOTE_URL", "")
ENT_USER      = os.getenv("PRONOTE_USER", "")
ENT_PASS      = os.getenv("PRONOTE_PASS", "")

# Tes sélecteurs validés pour atteindre l’EDT
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()

# Onglets de semaines j_1, j_2, …
WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))

WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "1000"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

CALENDAR_ID   = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX  = os.getenv("TITLE_PREFIX", "[Mo] ")
COLOR_ID      = os.getenv("COLOR_ID", "6")
HEADFUL       = os.getenv("HEADFUL", "0") == "1"

# Fusion de créneaux consécutifs (même cours/salle, collés)
COALESCE_MINUTES = int(os.getenv("COALESCE_MINUTES", "5"))

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts (ms)
TIMEOUT_MS  = 120_000
SCREEN_DIR  = "screenshots"

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

# ---- Upsert idempotent via propriété étendue privée ----
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
        svc.events().update(calendarId=cal_id, eventId=existing["id"], body=body, sendUpdates="none").execute()
        return "updated"
    else:
        svc.events().insert(calendarId=cal_id, body=body, sendUpdates="none").execute()
        return "created"

# ========= Parsing de créneaux =========
HOUR_RE = re.compile(r'(?P<h>\d{1,2})\s*[:hH]\s*(?P<m>\d{2})')

def parse_timespan(text: str):
    times = HOUR_RE.findall(text or "")
    if len(times) >= 2:
        (h1,m1),(h2,m2) = times[0], times[1]
        return (int(h1),int(m1)), (int(h2),int(m2))
    return None

def clean_summary(label: str) -> str:
    s = " ".join((label or "").split())
    s = re.sub(r'^\s*\d{1,2}\s*[:hH]\s*\d{2}\s*[–\-à]\s*\d{1,2}\s*[:hH]\s*\d{2}\s*', '', s)
    s = re.sub(r'(Salle|Salles?).*$', '', s, flags=re.IGNORECASE)
    s = re.sub(r'(Prof\.?:.*)$', '', s, flags=re.IGNORECASE)
    return s.strip(" -–") or "Cours"

def extract_room(label: str) -> str:
    s = " ".join((label or "").split())
    m = re.search(r'(?:Salle|Salles?)\s*([A-Za-z0-9\-_. ]+)', s, re.IGNORECASE)
    return m.group(1).strip() if m else ""

# ========= Playwright utils =========
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
        const hasTimes = /\d{1,2}\s*[h:]\s*\d{2}/i.test(txt);
        return hasTitle && hasTimes;
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
                ok = frame.evaluate(r"""
                  (pat) => {
                    const rx = new RegExp(pat, 'i');
                    const nodes = Array.from(document.querySelectorAll('body *')).filter(n => (n.innerText||'').match(rx));
                    for (const n of nodes) {
                      let p=n;
                      while (p) {
                        if (p.tagName==='A' || p.tagName==='BUTTON' || p.getAttribute('role')==='button' || p.onclick) { p.click(); return true; }
                        p=p.parentElement;
                      }
                    }
                    return false;
                  }
                """, pat)
                if ok: return True
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

def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        click_text_anywhere(page, ["Tout voir", "Voir tout", "Tout afficher"])
        page.wait_for_timeout(400)

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
        raise RuntimeError("Impossible d’atteindre l’Emploi du temps.")

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

# ========= Extraction basée sur les colonnes (dates par jour) =========
def extract_week_items_with_dates(page_or_frame) -> Dict[str, Any]:
    """
    Dans la frame EDT :
    - détecte chaque colonne jour (ids *_contX ou structures proches),
    - lit la date affichée dans l'en-tête de la colonne (dd/mm ou dd/mm/yyyy),
    - récupère les tuiles (aria-label/title/texte) dans cette colonne,
    - produit {label, dayISO}.
    """
    if hasattr(page_or_frame, "frames"):
        fr = wait_timetable_any_frame(page_or_frame, timeout_ms=10_000)
    else:
        fr = page_or_frame

    # Essaie quand même de lire l'entête de semaine (utile en log)
    header_text = ""
    for sel in ['text=/Semaine .* au .*/', '.titrePeriode', '.zoneSemaines', 'header']:
        try:
            loc = fr.locator(sel)
            if loc.count() > 0:
                header_text = (loc.first.inner_text() or "").strip()
                if header_text:
                    break
        except:
            pass

    js_result = fr.evaluate(r"""
    () => {
      const out = { header: "", days: [] };

      const headNode = document.querySelector('.titrePeriode, .zoneSemaines, header');
      if (headNode) out.header = (headNode.innerText || '').trim();

      // Heuristique : colonnes jour souvent *_contX (0..6) ou éléments contenant un libellé avec une date.
      const dayCols = Array.from(document.querySelectorAll('[id*="_cont"], [class*="col"], [class*="Jour"]'));

      const rxDate = /\b(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?\b/;
      const rxTime = /\b\d{1,2}\s*(?:[:hH]|heures?)\s*\d{2}\s*(?:-|–|à)\s*\d{1,2}\s*(?:[:hH]|heures?)\s*\d{2}\b/i;

      const pickDateText = (col) => {
        // chercher d'abord dans des en-têtes proches
        const candidates = [];
        if (col.previousElementSibling) candidates.push(col.previousElementSibling);
        if (col.firstElementChild) candidates.push(col.firstElementChild);
        candidates.push(col);

        for (const c of candidates) {
          const txt = (c.innerText || '').replace(/\s+/g,' ').trim();
          const m = txt.match(rxDate);
          if (m) return m[0];
        }
        // fallback: chercher dans toute la colonne
        const m2 = (col.innerText || '').match(rxDate);
        if (m2) return m2[0];
        return null;
      };

      const findTilesInCol = (col) => {
        const tiles = [];
        const add = (el, label) => { if (label) tiles.push({ el, label: String(label).trim() }); };

        col.querySelectorAll('[aria-label]').forEach(el => { const v=el.getAttribute('aria-label'); if (v && rxTime.test(v)) add(el, v); });
        col.querySelectorAll('[title]').forEach(el => { const v=el.getAttribute('title');     if (v && rxTime.test(v)) add(el, v); });

        // texte brut court
        col.querySelectorAll('*').forEach(el => {
          const t = (el.innerText || '').trim();
          if (t && t.length < 220 && rxTime.test(t)) add(el, t);
        });

        // dédup local par (label)
        const seen = new Set(); const res = [];
        for (const it of tiles) { if (!seen.has(it.label)) { seen.add(it.label); res.push({label: it.label}); } }
        return res;
      };

      for (const col of dayCols) {
        const dateText = pickDateText(col);
        if (!dateText) continue;
        const tiles = findTilesInCol(col);
        if (tiles.length === 0) continue;
        out.days.push({ dateText, tiles });
      }

      // dédup global: même date + label
      const gseen = new Set(); const gdays = [];
      for (const d of out.days) {
        const uniqTiles = [];
        for (const t of d.tiles) {
          const k = d.dateText + '|' + t.label;
          if (!gseen.has(k)) { gseen.add(k); uniqTiles.push(t); }
        }
        if (uniqTiles.length) gdays.push({ dateText: d.dateText, tiles: uniqTiles });
      }
      out.days = gdays;
      return out;
    }
    """)

    return js_result  # {header, days:[{dateText, tiles:[{label}]}]}

def guess_year_for_month(month: int) -> int:
    now = datetime.now()
    # année scolaire : si mois <= 7 (janv–juil) et qu'on est en fin d'année (août–déc), c'est probablement l'année suivante
    if month <= 7 and now.month >= 8:
        return now.year + 1
    return now.year

def to_dt_from_date_and_hm(date_ddmm: str, hm: tuple) -> Optional[datetime]:
    try:
        d, m = [int(x) for x in date_ddmm.split("/")]
        y = guess_year_for_month(m)
        return datetime(y, m, d, hm[0], hm[1], 0)
    except Exception:
        return None

def coalesce(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not entries:
        return entries
    items = sorted(entries, key=lambda e: (e["date"], e["title"], e["room"], e["start"]))
    merged = []

    def minutes(dt: datetime) -> int:
        return int(dt.timestamp() // 60)

    for e in items:
        if not merged:
            merged.append(e); continue
        last = merged[-1]
        if (e["date"] == last["date"] and
            e["title"] == last["title"] and
            (e["room"] or "") == (last["room"] or "")):
            gap = minutes(e["start"]) - minutes(last["end"])
            if 0 <= gap <= COALESCE_MINUTES:
                last["end"] = max(last["end"], e["end"])
                continue
        merged.append(e)
    return merged

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

        # ENT → PRONOTE → Emploi du temps
        login_ent(page)
        pronote = open_pronote(context, page)
        goto_timetable(pronote)
        ensure_all_visible(pronote)

        # Parcours des semaines (onglets j_n)
        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1

        for week_idx in range(start_idx, end_idx + 1):
            used = goto_week_by_index(pronote, week_idx)
            ensure_all_visible(pronote)

            data = extract_week_items_with_dates(pronote)  # {header, days:[{dateText, tiles:[{label}]}]}
            hdr  = (data.get("header") or "").replace("\n"," ")[:160]
            nb   = sum(len(d["tiles"]) for d in data.get("days", []))
            print(f"Semaine {week_idx}: {nb} cours, header='{hdr}'")

            entries: List[Dict[str, Any]] = []
            for day in data.get("days", []):
                date_txt = day["dateText"]  # ex "08/09" ou "08/09/2025"
                for t in day["tiles"]:
                    label = (t.get("label") or "").strip()
                    ts = parse_timespan(label)
                    if not ts:
                        continue
                    (h1,m1),(h2,m2) = ts
                    start_dt = to_dt_from_date_and_hm(date_txt, (h1,m1))
                    end_dt   = to_dt_from_date_and_hm(date_txt, (h2,m2))
                    if not start_dt or not end_dt:
                        continue

                    now = datetime.now()
                    if end_dt < (now - timedelta(days=21)) or start_dt > (now + timedelta(days=120)):
                        continue

                    title = f"{TITLE_PREFIX}{clean_summary(label)}"
                    room  = extract_room(label)

                    entries.append({
                        "title": title,
                        "room": room,
                        "start": start_dt,
                        "end": end_dt,
                        "date": start_dt.date(),
                    })

            entries = coalesce(entries)

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
        os.makedirs(SCREEN_DIR, exist_ok=True)
    except:
        pass
    try:
        run()
    except Exception as ex:
        print(f"[FATAL] {ex}")
        sys.exit(1)
