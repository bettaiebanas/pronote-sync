# pronote_playwright_to_family_mo.py
# SPDX-License-Identifier: MIT
from __future__ import annotations

import os
import re
import sys
import time
import hashlib
import unicodedata
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
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
TITLE_PREFIX  = os.getenv("TITLE_PREFIX", "[Mo] ")
COLOR_ID      = os.getenv("COLOR_ID", "6")
HEADFUL       = os.getenv("HEADFUL", "0") == "1"

TIMETABLE_PRE_SELECTOR = os.getenv("TIMETABLE_PRE_SELECTOR", "").strip()
TIMETABLE_SELECTOR     = os.getenv("TIMETABLE_SELECTOR", "").strip()
TIMETABLE_FRAME        = os.getenv("TIMETABLE_FRAME", "").strip()
WEEK_TAB_TEMPLATE      = os.getenv("WEEK_TAB_TEMPLATE", "#GInterface\\.Instances\\[2\\]\\.Instances\\[0\\]_j_{n}").strip()

FETCH_WEEKS_FROM       = int(os.getenv("FETCH_WEEKS_FROM", "1"))
WEEKS_TO_FETCH         = int(os.getenv("WEEKS_TO_FETCH", "4"))
CLICK_TOUT_VOIR        = os.getenv("CLICK_TOUT_VOIR", "1") == "1"
WAIT_AFTER_NAV_MS      = int(os.getenv("WAIT_AFTER_NAV_MS", "800"))

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES           = ["https://www.googleapis.com/auth/calendar"]
TIMEZONE         = "Europe/Paris"

TIMEOUT_MS  = 120_000
SCREEN_DIR  = "screenshots"

# ===================== IO-safe console =====================
try:
    # Force UTF-8 console where supported to avoid 'charmap' errors on Windows runners
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ===================== Utils =====================
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    try:
        print(line)
    except UnicodeEncodeError:
        # Fallback ASCII
        safe = (line.encode("ascii", "replace")).decode("ascii")
        print(safe)

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\\s+", " ", s).strip().lower()
    return s

def make_event_id(start: datetime, end: datetime, title: str, location: str) -> str:
    key = f"{start.isoformat()}|{end.isoformat()}|{_norm(title)}|{_norm(location)}"
    return "prn_" + hashlib.sha1(key.encode()).hexdigest()[:24]

def _safe_mkdir(p: str) -> None:
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass

def _safe_shot(page, name: str) -> None:
    try:
        _safe_mkdir(SCREEN_DIR)
        page.screenshot(path=f"{SCREEN_DIR}/{name}.png", full_page=True)
    except Exception:
        pass

# ===================== Google Calendar =====================
def get_gcal_service():
    if not CALENDAR_ID:
        raise SystemExit("CALENDAR_ID manquant.")

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
            log(f"[Google OAuth] {e}")
            raise
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

def upsert_event_by_id(svc, cal_id: str, event_id: str, body: Dict[str, Any]) -> str:
    try:
        svc.events().get(calendarId=cal_id, eventId=event_id).execute()
        svc.events().patch(calendarId=cal_id, eventId=event_id, body=body, sendUpdates="none").execute()
        return "updated"
    except HttpError as e:
        status = getattr(getattr(e, "resp", None), "status", None)
        if status == 404:
            body2 = dict(body); body2["id"] = event_id
            svc.events().insert(calendarId=cal_id, body=body2, sendUpdates="none").execute()
            return "created"
        raise

# ===================== Parsing =====================
HOUR_RE = re.compile(r'(?P<h>\\d{1,2})[:hH](?P<m>\\d{2})')

def parse_timespan(text: str):
    times = HOUR_RE.findall(text or "")
    if len(times) >= 2:
        (h1, m1), (h2, m2) = times[0], times[1]
        return (int(h1), int(m1)), (int(h2), int(m2))
    return None

def parse_aria_label(label: str) -> Dict[str, Any]:
    d: Dict[str, Any] = {"start": None, "end": None, "summary": None, "room": ""}
    lab = " ".join((label or "").split())

    tspan = parse_timespan(lab)
    if tspan:
        d["start"], d["end"] = tspan

    m_room = re.search(r'(?:Salle|Salles?)\\s*([A-Za-z0-9\\-_. ]+)', lab, re.IGNORECASE)
    if m_room:
        d["room"] = m_room.group(1).strip()

    summary = lab
    summary = re.sub(r'^\\s*\\d{1,2}[:hH]\\d{2}\\s*[–\\-]\\s*\\d{1,2}[:hH]\\d{2}\\s*', '', summary)
    summary = re.sub(r'(Salle|Salles?).*$', '', summary, flags=re.IGNORECASE)
    summary = re.sub(r'(Prof\\.?\\s?:.*)$', '', summary, flags=re.IGNORECASE)
    summary = summary.strip(" -–")
    d["summary"] = summary if summary else "Cours"
    return d

def to_datetime(base_monday: Optional[datetime], day_idx: Optional[int], hm: tuple[int, int]) -> datetime:
    if base_monday is not None and day_idx is not None and 0 <= int(day_idx) <= 6:
        base = base_monday + timedelta(days=int(day_idx))
    else:
        base = datetime.now()
    return base.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

# ===================== Playwright helpers =====================
def _iter_contexts(page):
    yield page
    for fr in page.frames:
        yield fr

def first_locator_any(page, selectors: List[str]):
    for ctx in _iter_contexts(page):
        for sel in selectors:
            try:
                loc = ctx.locator(sel)
                if loc.count() > 0:
                    return loc.first
            except Exception:
                continue
    return None

def click_first_any(page, selectors: List[str]) -> bool:
    loc = first_locator_any(page, selectors)
    if not loc:
        return False
    try:
        loc.click()
        return True
    except Exception:
        try:
            el = loc.element_handle()
            if el:
                el.evaluate("(n)=>n.click()")
                return True
        except Exception:
            pass
        return False

def accept_cookies_any(page) -> None:
    texts = ["Tout accepter","Accepter tout","J'accepte","Accepter","OK","Continuer","J'ai compris"]
    sels = [f'button:has-text(\"{t}\")' for t in texts] + [f'role=button[name=\"{t}\"]' for t in texts]
    click_first_any(page, sels)

def _frame_has_timetable_js() -> str:
    return r"""
      () => {
        const txt = (document.body.innerText || '').replace(/\s+/g,' ');
        const hasTitle = /Emploi du temps/i.test(txt) || /Planning|Agenda/i.test(txt);
        const hasWeek  = /(Semaine|du\s+\d{1,2}\/\d{1,2}\/\d{4}\s+au\s+\d{1,2}\/\d{1,2}\/\d{4})/i.test(txt);
        const hasTimes = /\d{1,2}\s*[h:]\s*\d{2}/i.test(txt);
        return (hasTitle && (hasTimes || hasWeek)) || (hasWeek && hasTimes);
      }
    """

def wait_timetable_any(page, timeout_ms: int = TIMEOUT_MS):
    deadline = time.time() + timeout_ms/1000.0
    js = _frame_has_timetable_js()
    while time.time() < deadline:
        for ctx in _iter_contexts(page):
            try:
                if ctx.evaluate(js):
                    return ctx
            except Exception:
                pass
        page.wait_for_timeout(300)
    raise TimeoutError("Timetable not found")

def click_text_anywhere(page, patterns: List[str]) -> bool:
    for ctx in _iter_contexts(page):
        for pat in patterns:
            for loc in [
                ctx.get_by_role("link", name=re.compile(pat, re.I)),
                ctx.get_by_role("button", name=re.compile(pat, re.I)),
            ]:
                try:
                    if loc.count() > 0:
                        loc.first.click()
                        return True
                except Exception:
                    pass
            try:
                found = ctx.evaluate(
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
            except Exception:
                pass
    return False

def click_css_any(page, css: str, screenshot_tag: str = "") -> bool:
    if not css:
        return False
    for ctx in _iter_contexts(page):
        try:
            loc = ctx.locator(css)
            if loc.count() > 0:
                try:
                    loc.first.scroll_into_view_if_needed()
                except Exception:
                    pass
                try:
                    loc.first.click()
                except Exception:
                    try:
                        el = loc.first.element_handle()
                        if el:
                            el.evaluate("(n)=>n.click()")
                        else:
                            continue
                    except Exception:
                        continue
                page.wait_for_timeout(WAIT_AFTER_NAV_MS)
                if screenshot_tag:
                    _safe_shot(page, f"08-clicked-{screenshot_tag}")
                return True
        except Exception as e:
            log(f"[NAV] click_css_any fail: {e}")
    return False

# ===================== Navigation =====================
def login_ent(page) -> None:
    _safe_mkdir(SCREEN_DIR)
    page.set_default_timeout(TIMEOUT_MS)
    page.goto(ENT_URL, wait_until="load")
    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    _safe_shot(page, "01-ent-welcome")

    click_first_any(page, [
        'a:has-text(\"Se connecter\")','a:has-text(\"Connexion\")',
        'button:has-text(\"Se connecter\")','button:has-text(\"Connexion\")',
        'a[href*=\"login\"]','a[href*=\"auth\"]'
    ])
    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    _safe_shot(page, "02-ent-after-click-login")

    user_candidates = [
        'input[name=\"email\"]','input[name=\"username\"]','#username',
        'input[type=\"text\"][name*=\"user\"]','input[type=\"text\"]','input[type=\"email\"]',
        'input#email','input[name=\"login\"]','input[name=\"j_username\"]'
    ]
    pass_candidates = [
        'input[type=\"password\"][name=\"password\"]','#password','input[type=\"password\"]','input[name=\"j_password\"]'
    ]
    submit_candidates = [
        'button[type=\"submit\"]','input[type=\"submit\"]',
        'button:has-text(\"Se connecter\")','button:has-text(\"Connexion\")','button:has-text(\"Valider\")'
    ]

    user_loc = first_locator_any(page, user_candidates)
    pass_loc = first_locator_any(page, pass_candidates)
    if not user_loc or not pass_loc:
        click_first_any(page, [
            'button:has-text(\"Identifiant\")','a:has-text(\"Identifiant\")',
            'button:has-text(\"Compte\")','a:has-text(\"Compte\")','a:has-text(\"ENT\")'
        ])
        page.wait_for_load_state("domcontentloaded")
        accept_cookies_any(page)
        user_loc = first_locator_any(page, user_candidates)
        pass_loc = first_locator_any(page, pass_candidates)

    if not user_loc or not pass_loc:
        _safe_shot(page, "03-ent-no-fields")
        raise RuntimeError("Champ identifiant ENT introuvable.")

    user_loc.fill(ENT_USER)
    pass_loc.fill(ENT_PASS)
    if not click_first_any(page, submit_candidates):
        user_loc.press("Enter")

    page.wait_for_load_state("domcontentloaded")
    accept_cookies_any(page)
    _safe_shot(page, "05-ent-after-submit")

def open_pronote(context, page):
    page.set_default_timeout(TIMEOUT_MS)
    if PRONOTE_URL:
        page.goto(PRONOTE_URL, wait_until="load")
        page.wait_for_load_state("domcontentloaded")
        accept_cookies_any(page)
        _safe_shot(page, "06-pronote-direct")
        return page

    with page.expect_popup() as p:
        clicked = click_first_any(page, [
            'a:has-text(\"PRONOTE\")','a[title*=\"PRONOTE\"]','a[href*=\"pronote\"]','text=PRONOTE'
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

def goto_timetable(pronote_page):
    pronote_page.set_default_timeout(TIMEOUT_MS)
    accept_cookies_any(pronote_page)

    if TIMETABLE_PRE_SELECTOR:
        click_css_any(pronote_page, TIMETABLE_PRE_SELECTOR, "pre-selector")
    if TIMETABLE_SELECTOR:
        if click_css_any(pronote_page, TIMETABLE_SELECTOR, "timetable-selector"):
            accept_cookies_any(pronote_page)
            try:
                ctx = wait_timetable_any(pronote_page, timeout_ms=30_000)
                _safe_shot(pronote_page, "08-timetable-custom-selector")
                return ctx
            except TimeoutError:
                _safe_shot(pronote_page, "08-timetable-custom-timeout")

    try:
        ctx = wait_timetable_any(pronote_page, timeout_ms=10_000)
        _safe_shot(pronote_page, "08-timetable-already-here")
        return ctx
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
                    ctx = wait_timetable_any(pronote_page, timeout_ms=30_000)
                    _safe_shot(pronote_page, f"08-timetable-ready-{i}-{pat}")
                    return ctx
                except TimeoutError:
                    _safe_shot(pronote_page, f"08-not-ready-{i}-{pat}")
        pronote_page.wait_for_timeout(600)

    try:
        ctx = wait_timetable_any(pronote_page, timeout_ms=15_000)
        _safe_shot(pronote_page, "08-timetable-ready-fallback")
        return ctx
    except TimeoutError:
        _safe_shot(pronote_page, "08-timetable-NOT-found")
        raise RuntimeError("Impossible d'atteindre l'Emploi du temps.")

def ensure_all_visible(page) -> None:
    if CLICK_TOUT_VOIR:
        click_text_anywhere(page, ["Tout voir", "Voir tout", "Tout afficher"])
        page.wait_for_timeout(400)

def goto_week_by_index(page, n: int) -> bool:
    if not WEEK_TAB_TEMPLATE:
        return False
    css = WEEK_TAB_TEMPLATE.format(n=n)
    ok = click_css_any(page, css, f"week-{n}")
    if ok:
        try:
            wait_timetable_any(page, timeout_ms=20_000)
        except TimeoutError:
            pass
    return ok

def extract_week_info(pronote_page) -> Dict[str, Any]:
    ctx = wait_timetable_any(pronote_page, timeout_ms=30_000)
    header_text = ctx.evaluate(r"""
      () => {
        const txt = (document.body.innerText || '').replace(/\s+/g,' ');
        const m = txt.match(/du\s+\d{2}\/\d{2}\/\d{4}\s+au\s+\d{2}\/\d{2}\/\d{4}/i);
        return m ? m[0] : '';
      }
    """)
    tiles = ctx.evaluate(r"""
      () => {
        const out = [];
        const add = (el, label) => {
          if (!label) return;
          let dayIndex = null, p = el;
          while (p) {
            if (p.getAttribute && p.getAttribute('data-dayindex')) {
              const v = parseInt(p.getAttribute('data-dayindex'));
              if (!Number.isNaN(v)) { dayIndex = v; }
              break;
            }
            p = p.parentElement;
          }
          out.push({ label, dayIndex });
        };
        const rx = /(\d{1,2}\s*[h:]\s*\d{2}).{0,80}(\d{1,2}\s*[h:]\s*\d{2})/i;
        document.querySelectorAll('[aria-label],[title]').forEach(e => {
          const v = e.getAttribute('aria-label') || e.getAttribute('title') || '';
          if (rx.test(v)) add(e, v);
        });
        Array.from(document.querySelectorAll('body *')).forEach(e => {
          const t = (e.innerText || '').trim();
          if (t && t.length < 260 && rx.test(t)) add(e, t);
        });
        return out;
      }
    """)
    if not tiles or len(tiles) < 3:
        extra = ctx.evaluate(r"""
          () => {
            const out = [];
            const rx = /(\d{1,2}\s*[h:]\s*\d{2}).{0,100}(\d{1,2}\s*[h:]\s*\d{2})/i;
            const pushIf = (el) => {
              if (!el) return;
              const txt = (el.innerText || '').replace(/\s+/g,' ').trim();
              if (txt && rx.test(txt)) {
                out.push({ label: txt, dayIndex: null });
              }
            };
            document.querySelectorAll('[id^="id_"][id*="coursInt_"] table, [id^="id_"][id*="cont"]').forEach(pushIf);
            return out;
          }
        """)
        tiles = (tiles or []) + (extra or [])

    d0 = None
    try:
        m = re.search(r'(\\d{2}/\\d{2}/\\d{4}).*?(\\d{2}/\\d{2}/\\d{4})', header_text or '')
        if m:
            d0 = datetime.strptime(m.group(1), "%d/%m/%Y")
    except Exception:
        d0 = None
    return {"monday": d0, "tiles": tiles, "header": header_text}

def iter_next_week(pronote_page) -> bool:
    if click_first_any(pronote_page, [
        'button[title*=\"suivante\"]','button[aria-label*=\"suivante\"]','button:has-text(\">\")',
        'a[title*=\"suivante\"]','a:has-text(\"Semaine suivante\")'
    ]):
        accept_cookies_any(pronote_page)
        wait_timetable_any(pronote_page)
        _safe_shot(pronote_page, "09-pronote-next-week")
        return True
    return False

# ===================== Main =====================
def run() -> None:
    if not ENT_USER or not ENT_PASS:
        raise SystemExit("PRONOTE_USER / PRONOTE_PASS manquants.")

    svc = get_gcal_service()
    created = updated = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context = browser.new_context(locale="fr-FR", timezone_id=TIMEZONE)
        page = context.new_page(); page.set_default_timeout(TIMEOUT_MS)

        log("Connexion ENT...")
        login_ent(page)
        log("Ouverture PRONOTE...")
        pronote = open_pronote(context, page)
        log("Navigation vers 'Emploi du temps'...")
        goto_timetable(pronote)

        start_idx = max(1, FETCH_WEEKS_FROM)
        end_idx   = start_idx + max(1, WEEKS_TO_FETCH) - 1

        for week_idx in range(start_idx, end_idx + 1):
            log(f"-> Selection Semaine index={week_idx} via css '{WEEK_TAB_TEMPLATE.format(n=week_idx)}'")
            used_tab = goto_week_by_index(pronote, week_idx)
            accept_cookies_any(pronote)
            ensure_all_visible(pronote)

            info  = extract_week_info(pronote)
            d0    = info["monday"]
            tiles = info["tiles"] or []
            hdr   = (info.get("header") or "").replace("\\n", " ")[:160]
            log(f"Semaine {week_idx}: {len(tiles)} cases, header='{hdr}'")

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

                title    = f"{TITLE_PREFIX}{(parsed['summary'] or 'Cours').strip()}"
                event_id = make_event_id(start_dt, end_dt, title, parsed["room"])
                event = {
                    "summary": title,
                    "location": parsed["room"],
                    "start": {"dateTime": start_dt.isoformat(), "timeZone": TIMEZONE},
                    "end":   {"dateTime": end_dt.isoformat(),   "timeZone": TIMEZONE},
                    "colorId": COLOR_ID,
                    "extendedProperties": {"private": {"source": "pronote_playwright"}},
                }
                try:
                    action = upsert_event_by_id(svc, CALENDAR_ID, event_id, event)
                    if action == "created":
                        created += 1
                    else:
                        updated += 1
                except HttpError as e:
                    log(f"[GCAL] {e}")

            if not used_tab and week_idx < end_idx:
                if not iter_next_week(pronote):
                    break

        browser.close()

    log(f"Termine. crees={created}, maj={updated}")

if __name__ == "__main__":
    try:
        run()
    except Exception as ex:
        _safe_mkdir(SCREEN_DIR)
        log(f"[FATAL] {ex}")
        sys.exit(1)
