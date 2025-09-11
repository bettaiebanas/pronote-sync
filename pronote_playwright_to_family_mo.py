# pronote_playwright_to_family_mo.py
import os, sys, re, hashlib, unicodedata, time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========= CONFIG (via env) =========
ENT_URL       = os.getenv("ENT_URL", "https://ent77.seine-et-marne.fr/welcome")
PRONOTE_URL   = os.getenv("PRONOTE_URL", "")                    # si vide: on clique la tuile PRONOTE depuis l’ENT
ENT_USER      = os.getenv("PRONOTE_USER", "")
ENT_PASS      = os.getenv("PRONOTE_PASS", "")

# Chemins d’accès spécifiques (facultatifs) fournis par toi
TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()   # ex: '#GInterface\\.Instances\\[0\\]\\.Instances\\[1\\]_Combo5'
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()       # ex: '#GInterface\\.Instances\\[0\\]\\.Instances\\[1\\]_Liste_niveau5 > ul > li:nth-child(1) > div > div'
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()          # ex: 'parent.html' (sous-chaîne d’URL de la frame)

WEEK_TAB_TEMPLATE = os.getenv("WEEK_TAB_TEMPLATE", "").strip()             # ex: '#GInterface\\.Instances\\[2\\]\\.Instances\\[0\\]_j_{n}'
FETCH_WEEKS_FROM  = int(os.getenv("FETCH_WEEKS_FROM", "1"))                # première semaine à cliquer
WEEKS_TO_FETCH    = int(os.getenv("WEEKS_TO_FETCH", "4"))                  # nombre total de semaines à traiter
WAIT_AFTER_NAV_MS = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))
CLICK_TOUT_VOIR   = os.getenv("CLICK_TOUT_VOIR", "1") == "1"

# Google Calendar
CALENDAR_ID   = os.getenv("CALENDAR_ID", "family15066434840617961429@group.calendar.google.com")
TITLE_PREFIX  = "[Mo] "
COLOR_ID      = "6"     # orange
HEADFUL       = os.getenv("HEADFUL", "0") == "1"

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Timeouts & captures
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

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def make_event_id(start: datetime, end: datetime, title: str, location: str) -> str:
    key = f"{start.isoformat()}|{end.isoformat()}|{_norm(title)}|{_norm(location)}"
    return "prn_" + hashlib.sha1(key.encode()).hexdigest()[:24]

def upsert_event_by_id(svc, cal_id: str, event_id: str, body: Dict[str, Any]) -> str:
    """get -> patch ; si 404 -> insert avec body incluant 'id'"""
    try:
        svc.events().get(calendarId=cal_id, eventId=event_id).execute()
        svc.events().patch(calendarId=cal_id, eventId=event_id, body=body, sendUpdates="none").execute()
        return "updated"
    except HttpError as e:
        if getattr(e, "resp", None) and e.resp.status == 404:
            body2 = dict(body)
            body2["id"] = event_id   # <- ID imposé ici (PAS en paramètre)
            svc.events().insert(calendarId=cal_id, body=body2, sendUpdates="none").execute()
            return "created"
        raise

# ========= Parsing =========
# Supporte '09:05' / '9h05' / '9 h 05' / '9 heures 05'
HOUR_RE = re.compile(r'(?P<h>\d{1,2})\s*(?:[:hH]|heures?)\s*(?P<m>\d{2})', re.I)

def parse_timespan(text: str):
    times = HOUR_RE.findall(text or "")
    if len(times) >= 2:
        (h1, m1), (h2, m2) = times[0], times[1]
        return (int(h1), int(m1)), (int(h2), int(m2))
    return None

def parse_aria_label(label: str) -> Dict[str, Any]:
    d = {"start": None, "end": None, "summary": None, "room": ""}
    lab = " ".join((label or "").split())

    # horaires
    tspan = parse_timespan(lab)
    if tspan:
        d["start"], d["end"] = tspan

    # salle simple (si dispo dans le texte)
    m_room = re.search(r'(?:Salle|Salles?)\s*([A-Za-z0-9\-_. ]+)', lab, re.I)
    if m_room:
        d["room"] = m_room.group(1).strip()

    # résumé
    summary = lab
    summary = re.sub(r'^\s*(Cours\s+du\s+[^:]+:?)', '', summary, flags=re.I)  # enlève "Cours du..."
    summary = re.sub(r'^\s*\d{1,2}\s*(?:[:hH]|heures?)\s*\d{2}\s*[–\-à]\s*\d{1,2}\s*(?:[:hH]|heures?)\s*\d{2}\s*', '', summary, flags=re.I)
    summary = re.sub(r'(Salle|Salles?).*$', '', summary, flags=re.I)
    summary = re.sub(r'(Prof\.?:.*)$', '', summary, flags=re.I)
    summary = summary.strip(" -–|")
    d["summary"] = summary if summary else "Cours"
    return d

def monday_of_week(text_header: str) -> Optional[datetime]:
    m = re.search(r'(\d{2}/\d{2}/\d{4}).*?(\d{2}/\d{2}/\d{4})', text_header or "")
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y")
        except:
            return None
    return None

def to_datetime(base_monday: Optional[datetime], day_idx: Optional[int], hm: tuple) -> Optional[datetime]:
    if hm is None:
        return None
    if base_monday is not None and day_idx is not None and 0 <= int(day_idx) <= 6:
        base = base_monday + timedelta(days=int(day_idx))
    else:
        # si on ne sait pas quel jour, on abandonne (mieux que de créer une mauvaise date)
        return None
    return base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

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

def _frame_has_timetable_js():
    return r"""
      () => {
        const txt = (document.body.innerText || '').replace(/\s+/g, ' ');
        const hasTitle = /Emploi du temps|Planning|Agenda/i.test(txt);
        const hasWeek  = /(Semaine|du\s+\d{1,2}\/\d{1,2}\/\d{4}\s+au\s+\d{1,2}\/\d{1,2}\/\d{4})/i.test(txt);
        const hasTimes = /\d{1,2}\s*(?:[:hH]|heures?)\s*\d{2}/i.test(txt);
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
        page.wait_for_timeout(300)
    raise TimeoutError("Timetable not found in any frame")

def click_text_anywhere(page, patterns: List[str]) -> bool:
    for fr in page.frames:
        for pat in patterns:
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
            try:
                ok = fr.evaluate(r"""
                    pat => {
                      const rx = new RegExp(pat, 'i');
                      const nodes = Array.from(document.querySelectorAll('body *')).filter(n => (n.innerText||'').match(rx));
                      for (const n of nodes) {
                        let p = n;
                        while (p) {
                          if (p.tagName==='A' || p.tagName==='BUTTON' || p.getAttribute('role')==='button' || p.onclick) { p.click(); return true; }
                          p = p.parentElement;
                        }
                      }
                      return false;
                    }
                """, pat)
                if ok: return True
            except:
                pass
    return False

def click_css_in_frames(page, css: str, frame_url_contains: str = "", screenshot_tag: str = "", wait_ms: int = None) -> bool:
    if not css:
        return False
    for fr in page.frames:
        if frame_url_contains and frame_url_contains not in fr.url:
            continue
        try:
            loc = fr.locator(css)
            if loc.count() > 0:
                loc.first.click()
                page.wait_for_timeout(wait_ms or WAIT_AFTER_NAV_MS)
                if screenshot_tag:
                    try: page.screenshot(path=f"{SCREEN_DIR}/clicked-{screenshot_tag}.png", full_page=True)
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
        raise RuntimeError("Champ identifiant ENT introuvable (mets HEADFUL=1 pour ajuster).")

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

    with page.expect_popup() as pinfo:
        clicked = click_first_in_frames(page, [
            'a:has-text("PRONOTE")','a[title*="PRONOTE"]','a[href*="pronote"]','text=PRONOTE'
        ])
        if not clicked:
            page.screenshot(path=f"{SCREEN_DIR}/06-pronote-tile-not-found.png", full_page=True)
            raise RuntimeError("Tuile/lien PRONOTE introuvable après login ENT.")
    try:
        pronote_page = pinfo.value
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

    # chemin explicite (si fourni)
    if TIMETABLE_PRE_SELECTOR:
        click_css_in_frames(pronote_page, TIMETABLE_PRE_SELECTOR, TIMETABLE_FRAME, "pre-selector")
    if TIMETABLE_SELECTOR:
        if click_css_in_frames(pronote_page, TIMETABLE_SELECTOR, TIMETABLE_FRAME, "timetable-selector"):
            accept_cookies_any(pronote_page)
            try:
                fr = wait_timetable_any_frame(pronote_page, timeout_ms=30_000)
                pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-custom.png", full_page=True)
                return fr
            except TimeoutError:
                pass

    # déjà sur l'EDT ?
    try:
        fr = wait_timetable_any_frame(pronote_page, timeout_ms=10_000)
        pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-already.png", full_page=True)
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
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-found-{i}-{pat}.png", full_page=True)
                    return fr
                except TimeoutError:
                    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-not-ready-{i}-{pat}.png", full_page=True)
        pronote_page.wait_for_timeout(500)

    pronote_page.screenshot(path=f"{SCREEN_DIR}/08-timetable-NOT-found.png", full_page=True)
    raise RuntimeError("Impossible d’atteindre l’Emploi du temps.")

def ensure_all_visible(page, frame=None):
    if CLICK_TOUT_VOIR:
        click_text_anywhere(page, ["Tout voir", "Voir tout", "Tout afficher"])
        if frame:
            try:
                frame.evaluate("() => {}")  # ping frame
            except:
                pass
        page.wait_for_timeout(300)

def goto_week_by_index(page, n: int, frame=None) -> bool:
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)

    # 1) côté page (toutes frames)
    if click_css_in_frames(page, css, TIMETABLE_FRAME, f"week-{n}"):
        return True

    # 2) en secours: dans la frame si possible
    if frame:
        try:
            loc = frame.locator(css)
            if loc.count() > 0:
                loc.first.click()
                page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                return True
        except:
            pass
    return False

def extract_week_info(frame) -> Dict[str, Any]:
    """Extrait l’en-tête de semaine + toutes les tuiles de cours depuis la *frame* EDT.
    Déduit le dayIndex depuis un ancêtre dont l'id correspond à ..._cours_<n>.
    """
    header_text = ""
    for sel in ['text=/Semaine .* au .*/', '.titrePeriode', '.zoneSemaines', 'header']:
        try:
            loc = frame.locator(sel)
            if loc.count() > 0:
                header_text = (loc.first.inner_text() or "").strip()
                if header_text:
                    break
        except:
            pass

    d0 = monday_of_week(header_text)

    tiles = frame.evaluate(r"""
    () => {
      const out = [];
      const rxTime = /\d{1,2}\s*(?:[:hH]|heures?)\s*\d{2}\s*(?:-|–|à)\s*\d{1,2}\s*(?:[:hH]|heures?)\s*\d{2}/i;

      const findDayIndex = (el) => {
        let p = el;
        while (p) {
          // cas PRONOTE: id ..._cours_<n>
          if (p.id && /_cours_(\d+)/.test(p.id)) {
            try { return parseInt(RegExp.$1, 10); } catch(e) {}
          }
          // fallback générique
          if (p.getAttribute && p.getAttribute('data-dayindex')) {
            try { return parseInt(p.getAttribute('data-dayindex'), 10); } catch(e) {}
          }
          p = p.parentElement;
        }
        return null;
      };

      const add = (el, label) => {
        if (!label) return;
        const lab = String(label).trim();
        if (!rxTime.test(lab)) return;
        const dayIndex = findDayIndex(el);
        out.push({ label: lab, dayIndex });
      };

      // 1) attributs explicites
      document.querySelectorAll('[aria-label]').forEach(el => {
        const v = el.getAttribute('aria-label');
        if (v) add(el, v);
      });
      document.querySelectorAll('[title]').forEach(el => {
        const v = el.getAttribute('title');
        if (v) add(el, v);
      });

      // 2) petits blocs de texte (sélecteurs fréquents sur l'EDT)
      document.querySelectorAll('td[id*="_cont"], div.NoWrap, div[class*="EmploiDuTemps"]').forEach(el => {
        const t = (el.innerText || '').trim();
        if (t && t.length < 220 && rxTime.test(t)) add(el, t);
      });

      // optionnel : dédoublonnage simple (texte + dayIndex)
      const seen = new Set();
      const dedup = [];
      for (const it of out) {
        const k = (it.dayIndex ?? 'x') + '|' + it.label;
        if (!seen.has(k)) { seen.add(k); dedup.push(it); }
      }
      return dedup;
    }
    """)

    return {"monday": d0, "tiles": tiles, "header": header_text}


def iter_next_week(page, frame) -> bool:
    # bouton "Semaine suivante"
    if click_first_in_frames(page, [
        'button[title*="suivante"]','button[aria-label*="suivante"]','button:has-text("→")',
        'a[title*="suivante"]','a:has-text("Semaine suivante")'
    ]):
        accept_cookies_any(page)
        try:
            fr = wait_timetable_any_frame(page, timeout_ms=20_000)
            return True
        except:
            return False
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

        # ENT → PRONOTE → Emploi du temps
        login_ent(page)
        pronote = open_pronote(context, page)
        frame = goto_timetable(pronote)
        ensure_all_visible(pronote, frame)

        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1

        for week_idx in range(start_idx, end_idx + 1):
            used_tab = goto_week_by_index(pronote, week_idx, frame)
            # ré-attacher la frame après changement éventuel
            try:
                frame = wait_timetable_any_frame(pronote, timeout_ms=20_000)
            except:
                pass
            ensure_all_visible(pronote, frame)

            info  = extract_week_info(frame)
            d0    = info["monday"]
            tiles = info["tiles"] or []
            hdr   = (info.get("header") or "").replace("\n", " ")[:140]
            print(f"Semaine {week_idx}: {len(tiles)} cours, header='{hdr}'")

            for t in tiles:
                label = (t.get("label") or "").strip()
                if not label:
                    continue
                parsed = parse_aria_label(label)
                day_idx = t.get("dayIndex")
                if day_idx is None:
                   print(f"[SKIP] pas de dayIndex pour: {label[:80]}")

                if not parsed["start"] or not parsed["end"]:
                    continue

            start_dt = to_datetime(d0, day_idx, parsed["start"])
            end_dt   = to_datetime(d0, day_idx, parsed["end"])

                if not start_dt or not end_dt:
                    continue

                now = datetime.now()
                if end_dt < (now - timedelta(days=21)) or start_dt > (now + timedelta(days=120)):
                    continue

                title = f"{TITLE_PREFIX}{(parsed['summary'] or 'Cours').strip()}"
                eid   = make_event_id(start_dt, end_dt, title, parsed["room"])

                event = {
                    "summary": title,
                    "location": parsed["room"],
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Paris"},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Europe/Paris"},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"source": "pronote_playwright"}},
                }

                try:
                    action = upsert_event_by_id(svc, CALENDAR_ID, eid, event)
                    if action == "created": created += 1
                    else: updated += 1
                except HttpError as e:
                    print(f"[GCAL] {e}")

            pronote.screenshot(path=f"{SCREEN_DIR}/week-{week_idx}.png", full_page=True)

            if not used_tab and week_idx < end_idx:
                if not iter_next_week(pronote, frame):
                    break
                try:
                    frame = wait_timetable_any_frame(pronote, timeout_ms=20_000)
                except:
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
