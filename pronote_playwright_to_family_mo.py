# pronote_playwright_to_family_mo.py
import os, sys, re, hashlib, unicodedata, time, json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

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

# Sélecteurs personnalisés (tes CSS relevés dans tes captures)
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()  # ex: " #GInterface\\.Instances\\[0\\]\\.Instances\\[1\\]_Combo5 "
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()      # ex: " #GInterface\\.Instances\\[0\\]\\.Instances\\[1\\]_Liste_niveau5 > ul > li:nth-child(1) > div > div "
TIMETABLE_FRAME_HINT   = os.getenv("TIMETABLE_FRAME", "").strip()         # indicatif d’URL de frame (facultatif)

# Onglets semaine j_1, j_2, j_3...
WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()            # ex: "#GInterface\\.Instances\\[2\\]\\.Instances\\[0\\]_j_{n}"

FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))               # 1 = semaine affichée
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH",   "4"))               # nombre d’onglets à parcourir

WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "1000"))           # pause après clic
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"              # cliquer “Tout voir”

# Google Calendar
CALENDAR_ID   = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX  = os.getenv("TITLE_PREFIX", "[Mo] ").strip()
COLOR_ID      = os.getenv("COLOR_ID", "6")   # 6 = orange

HEADFUL       = os.getenv("HEADFUL", "0") == "1"

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts (ms)
TIMEOUT_MS  = 120_000
SCREEN_DIR  = "screenshots"  # captures & dumps

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
    svc = build("calendar", "v3", credentials=creds)

    # Debug : s’assurer que le CALENDAR_ID existe bien côté Google
    try:
        lst = svc.calendarList().list().execute()
        ids = [it.get("id") for it in lst.get("items", [])]
        print(f"[DBG] CalendarList loaded: {len(ids)} calendars. CALENDAR_ID present? {CALENDAR_ID in ids}")
    except Exception as e:
        print(f"[DBG] CalendarList check failed: {e}")
    return svc

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
    # NB: on n’utilise pas eventId (invalide en insert), on s’appuie sur mo_hash
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

def parse_timespan(text: str) -> Optional[Tuple[Tuple[int,int], Tuple[int,int]]]:
    times = HOUR_RE.findall(text)
    if len(times) >= 2:
        (h1,m1),(h2,m2) = times[0], times[1]
        return (int(h1),int(m1)), (int(h2),int(m2))
    return None

def parse_aria_label(label: str) -> Dict[str, Any]:
    d = {"start": None, "end": None, "summary": None, "room": ""}
    lab = " ".join((label or "").split())
    tspan = parse_timespan(lab)
    if tspan:
        d["start"], d["end"] = tspan

    # Salle ...
    m_room = re.search(r'(?:Salle|Salles?)\s*([A-Za-z0-9\-_. ]+)', lab, re.IGNORECASE)
    if m_room:
        d["room"] = m_room.group(1).strip()

    # Résumé sans plages ni salle/prof
    summary = lab
    summary = re.sub(r'^\s*\d{1,2}[:hH]\d{2}\s*[–\-]\s*\d{1,2}[:hH]\d{2}\s*', '', summary)
    summary = re.sub(r'(Salle|Salles?).*$', '', summary, flags=re.IGNORECASE)
    summary = re.sub(r'(Prof\.?:.*)$', '', summary, flags=re.IGNORECASE)
    summary = summary.strip(" -–")
    d["summary"] = summary if summary else "Cours"
    return d

def monday_of_week(header_text: str) -> Optional[datetime]:
    # Ex: "Semaine A, du 01/09/2025 au 05/09/2025" → 01/09/2025
    m = re.search(r'du\s+(\d{1,2}/\d{1,2}/\d{4})\s+au\s+(\d{1,2}/\d{1,2}/\d{4})', header_text or "", re.IGNORECASE)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y")
        except:
            return None
    return None

def to_datetime(base_monday: Optional[datetime], day_idx: Optional[int], hm: tuple) -> datetime:
    if base_monday is not None and day_idx is not None and 0 <= int(day_idx) <= 6:
        base = base_monday + timedelta(days=int(day_idx))
    else:
        base = datetime.now()
    return base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

def merge_ranges_by_title_room(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fusionne les cours consécutifs strictement contigus (même résumé, même salle).
    entries: liste de dicts {"start": dt, "end": dt, "title": str, "room": str}
    """
    entries = sorted(entries, key=lambda x: (x["title"], x["room"], x["start"]))
    out = []
    for e in entries:
        if not out:
            out.append(e)
            continue
        last = out[-1]
        if (last["title"] == e["title"] and last["room"] == e["room"] and last["end"] == e["start"]):
            # étendre
            last["end"] = e["end"]
        else:
            out.append(e)
    return out

# ========= Playwright helpers =========
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

def dump_frames(pronote_page, tag: str):
    """Dump HTML de chaque frame + capture plein écran."""
    os.makedirs(SCREEN_DIR, exist_ok=True)
    try:
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-{tag}-page.png", full_page=True)
    except:
        pass
    for i, fr in enumerate(pronote_page.frames):
        try:
            html = fr.content()
            with open(f"{SCREEN_DIR}/dom-{tag}-{i}.html", "w", encoding="utf-8") as f:
                f.write(html)
            print(f"[DBG] dump frame#{i} url={fr.url} -> dom-{tag}-{i}.html")
        except Exception as e:
            print(f"[DBG] dump frame#{i} failed: {e}")

def find_css_in_any_frame(pronote_page, css: str, timeout_ms: int = 5000):
    """Retourne (frame, locator) si le sélecteur apparaît dans un des frames."""
    if not css:
        return (None, None)
    deadline = time.time() + timeout_ms/1000
    while time.time() < deadline:
        for fr in pronote_page.frames:
            if TIMETABLE_FRAME_HINT and TIMETABLE_FRAME_HINT not in fr.url:
                # si hint fourni, on restreint
                continue
            try:
                loc = fr.locator(css)
                if loc.count() > 0:
                    try:
                        loc.first.wait_for(state="visible", timeout=1000)
                    except:
                        pass
                    return (fr, loc.first)
            except:
                pass
        pronote_page.wait_for_timeout(200)
    return (None, None)

def frame_with_courses(pronote_page):
    """Détecte un frame qui contient des cases de cours (empreinte PRONOTE)."""
    js = r'() => !!document.querySelector(\'div[id*="_coursInt_"][aria-label]\')'
    for fr in pronote_page.frames:
        try:
            if TIMETABLE_FRAME_HINT and TIMETABLE_FRAME_HINT not in fr.url:
                continue
            if fr.evaluate(js):
                return fr
        except:
            pass
    return None

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

    # 0) déjà sur EDT ?
    fr = frame_with_courses(pronote_page)
    if fr:
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-already-here.png", full_page=True)
        print("[NAV] EDT déjà détecté (cases de cours).")
        return fr

    dump_frames(pronote_page, "before-edt")

    # 1) Pré-sélecteur puis sélecteur EDT (dans n'importe quel frame, éventuellement filtré par TIMETABLE_FRAME_HINT)
    clicked = False
    if TIMETABLE_PRE_SELECTOR:
        fr0, loc0 = find_css_in_any_frame(pronote_page, TIMETABLE_PRE_SELECTOR, timeout_ms=8000)
        print(f"[NAV] PRE_SELECTOR present? {bool(loc0)}")
        if loc0:
            try:
                fr0.evaluate("(e)=>e.scrollIntoView({block:'center'})", loc0)
            except: pass
            try:
                loc0.click()
                clicked = True
                pronote_page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-clicked-pre-selector.png", full_page=True)
            except Exception as e:
                print(f"[NAV] PRE_SELECTOR click error: {e}")

    if TIMETABLE_SELECTOR:
        fr1, loc1 = find_css_in_any_frame(pronote_page, TIMETABLE_SELECTOR, timeout_ms=8000)
        print(f"[NAV] TIMETABLE_SELECTOR present? {bool(loc1)}")
        if loc1:
            try:
                fr1.evaluate("(e)=>e.scrollIntoView({block:'center'})", loc1)
            except: pass
            try:
                loc1.click()
                clicked = True
                pronote_page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-clicked-timetable-selector.png", full_page=True)
            except Exception as e:
                print(f"[NAV] TIMETABLE_SELECTOR click error: {e}")

    if clicked:
        fr = frame_with_courses(pronote_page)
        if fr:
            pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-from-selectors.png", full_page=True)
            print("[NAV] EDT détecté après sélecteurs persos.")
            return fr
        else:
            print("[NAV] Après clics persos, pas encore de cases cours visibles…")

    # 2) Heuristiques texte
    attempts = [
        ["Emploi du temps", "Mon emploi du temps", "Emplois du temps"],
        ["Planning", "Agenda"],
        ["Vie scolaire", "Emploi du temps"],
    ]
    for i, pats in enumerate(attempts, 1):
        for pat in pats:
            if click_text_anywhere(pronote_page, [pat]):
                accept_cookies_any(pronote_page)
                pronote_page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                fr = frame_with_courses(pronote_page)
                if fr:
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-heuristic-{i}-{pat}.png", full_page=True)
                    print(f"[NAV] EDT détecté via heuristique '{pat}'.")
                    return fr
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-not-ready-{i}-{pat}.png", full_page=True)
        pronote_page.wait_for_timeout(600)

    # 3) Dernier check + dumps
    fr = frame_with_courses(pronote_page)
    if fr:
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-fallback.png", full_page=True)
        print("[NAV] EDT détecté en dernier recours.")
        return fr

    dump_frames(pronote_page, "edt-NOT-found")
    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-NOT-found.png", full_page=True)
    raise RuntimeError("Impossible d’atteindre l’Emploi du temps.")

def ensure_all_visible(page):
    if CLICK_TOUT_VOIR:
        # variantes possibles : “Tout voir”, “Voir tout”, …
        click_text_anywhere(page, ["Tout voir", "Voir tout", "Tout afficher"])
        page.wait_for_timeout(400)

def click_text_anywhere(page, patterns: List[str]) -> bool:
    """Clique un élément cliquable dont le texte matche un pattern, dans n’importe quel frame."""
    for fr in page.frames:
        for pat in patterns:
            # 1) rôles accessibles
            for loc in [
                fr.get_by_role("link", name=re.compile(pat, re.I)),
                fr.get_by_role("button", name=re.compile(pat, re.I)),
            ]:
                try:
                    if loc.count() > 0:
                        loc.first.click()
                        return True
                except:
                    pass
            # 2) fallback JS
            try:
                found = fr.evaluate(
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

def goto_week_by_index(pronote_page, n: int) -> bool:
    """Clique l’onglet j_n si WEEK_TAB_TEMPLATE est fourni."""
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)
    fr, loc = find_css_in_any_frame(pronote_page, css, timeout_ms=5000)
    if not loc:
        print(f"[NAV] Onglet semaine '{css}' introuvable.")
        return False
    try:
        fr.evaluate("(e)=>e.scrollIntoView({block:'center'})", loc)
    except:
        pass
    try:
        loc.click()
        pronote_page.wait_for_timeout(WAIT_AFTER_NAV_MS)
        # vérifier que l’EDT est encore là (il peut reloader)
        if not frame_with_courses(pronote_page):
            pronote_page.wait_for_timeout(600)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/09-week-tab-{n}.png", full_page=True)
        return True
    except Exception as e:
        print(f"[NAV] click onglet semaine {n} error: {e}")
        return False

# ========= Extraction =========
def extract_week_info_from_frame(edt_frame) -> Dict[str, Any]:
    """Lit header + tuiles DANS le frame de l’EDT UNIQUEMENT."""
    header_text = ""
    try:
        # 1) essayer des éléments de header connus
        for sel in ['.titrePeriode', '.zoneSemaines', 'header', 'legend', '[id*="Periode"]']:
            try:
                loc = edt_frame.locator(sel)
                if loc.count() > 0:
                    header_text = (loc.first.inner_text() or "").strip()
                    if header_text:
                        break
            except:
                pass
        # 2) fallback via body innerText
        if not header_text:
            header_text = edt_frame.evaluate(
                r"""() => (document.body.innerText || '').replace(/\s+/g,' ').trim()"""
            )
            # si trop verbeux, tenter d'extraire la partie "du xx/xx/xxxx au yy/yy/yyyy"
            m = re.search(r'(Semaine[^,]*,\s*du\s*\d{1,2}/\d{1,2}/\d{4}\s*au\s*\d{1,2}/\d{1,2}/\d{4})', header_text or "", re.IGNORECASE)
            if m:
                header_text = m.group(1)
            else:
                header_text = (header_text or "")[:200]
    except:
        pass

    # tiles dans le frame
    tiles = []
    try:
        tiles = edt_frame.evaluate(r"""
        () => {
          const out = [];
          const add = (el, label) => {
            if (!label) return;
            let dayIndex = null, p = el;
            while (p) {
              if (p.getAttribute && p.getAttribute('data-dayindex') != null) {
                dayIndex = parseInt(p.getAttribute('data-dayindex')); break;
              }
              p = p.parentElement;
            }
            out.push({ label, dayIndex });
          };
          const rx = /\d{1,2}[:hH]\d{2}.*\d{1,2}[:hH]\d{2}/;

          // PRONOTE: cases typiques
          document.querySelectorAll('div[id*="_coursInt_"][aria-label]').forEach(e => {
            const v = e.getAttribute('aria-label');
            if (v && rx.test(v)) add(e, v);
          });

          // fallback: attribut title
          document.querySelectorAll('[title]').forEach(e => {
            const v = e.getAttribute('title');
            if (v && rx.test(v)) add(e, v);
          });

          // dernier recours: innerText (limité pour éviter bruit)
          document.querySelectorAll('*').forEach(e => {
            const t = (e.innerText || '').trim();
            if (t && rx.test(t) && t.length < 180) add(e, t);
          });
          return out;
        }
        """)
    except Exception as e:
        print(f"[DBG] evaluate tiles error: {e}")
        tiles = []

    return {"monday": monday_of_week(header_text), "tiles": tiles or [], "header": header_text or ""}

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

        # ENT → PRONOTE → EDT
        login_ent(page)
        pronote = open_pronote(context, page)
        edt_frame = goto_timetable(pronote)  # frame où sont les cases

        # Parcours des semaines via onglets j_n (si fournis)
        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1
        print(f"[CFG] Weeks: {start_idx}..{end_idx}")

        for week_idx in range(start_idx, end_idx + 1):
            used_tab = goto_week_by_index(pronote, week_idx)
            accept_cookies_any(pronote)
            ensure_all_visible(pronote)

            # retrouver le frame EDT (il peut recharger après le clic)
            edt_frame = frame_with_courses(pronote) or edt_frame
            info  = extract_week_info_from_frame(edt_frame)
            d0    = info["monday"]
            tiles = info["tiles"] or []
            hdr   = (info.get("header") or "").replace("\n", " ")[:160]

            # Construire des entrées brutes
            raw_entries: List[Dict[str, Any]] = []
            for t in tiles:
                label = (t.get("label") or "").strip()
                parsed = parse_aria_label(label)
                if not parsed["start"] or not parsed["end"]:
                    continue
                start_dt = to_datetime(d0, t.get("dayIndex"), parsed["start"])
                end_dt   = to_datetime(d0, t.get("dayIndex"), parsed["end"])
                raw_entries.append({
                    "start": start_dt,
                    "end":   end_dt,
                    "title": (parsed["summary"] or "Cours").strip(),
                    "room":  parsed["room"] or "",
                })

            print(f"Semaine {week_idx}: {len(raw_entries)} cours, header='{hdr}'")
            print(f"[DBG]   entries construits: {len(raw_entries)}")

            # Fusion des blocs contigus même titre/salle
            merged = merge_ranges_by_title_room(raw_entries)
            print(f"[DBG]   après fusion: {len(merged)}")

            # Filtrer et pousser vers Google
            now = datetime.now()
            for e in merged:
                # filtre fenêtre temporelle raisonnable
                if e["end"] < (now - timedelta(days=60)) or e["start"] > (now + timedelta(days=180)):
                    continue

                title = f"{TITLE_PREFIX}{e['title']}"
                hash_id = make_hash_id(e["start"], e["end"], title, e["room"])
                event = {
                    "summary": title,
                    "location": e["room"],
                    "start": {"dateTime": e["start"].isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": e["end"].isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"mo_hash": hash_id, "source": "pronote_playwright"}},
                }
                try:
                    action = upsert_event_by_hash(svc, CALENDAR_ID, hash_id, event)
                    if action == "created":
                        created += 1
                    else:
                        updated += 1
                except HttpError as ehttp:
                    # Log lisible
                    try:
                        data = json.loads(ehttp.content.decode("utf-8"))
                    except:
                        data = str(ehttp)
                    print(f"[GCAL] HttpError: {data}")

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
