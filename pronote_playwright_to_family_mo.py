# -*- coding: utf-8 -*-
# pronote_playwright_to_family_mo.py

import os, re, json, hashlib, base64, time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict

from playwright.sync_api import sync_playwright

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ================== CONFIG (via env) ==================
ENT_URL       = os.environ["ENT_URL"]           # ex: https://ent77.seine-et-marne.fr/welcome
PRONOTE_URL   = os.environ["PRONOTE_URL"]       # ex: https://.../pronote/parent.html?identifiant=...
PRONOTE_USER  = os.environ["PRONOTE_USER"]
PRONOTE_PASS  = os.environ["PRONOTE_PASS"]

CAL_ID        = os.getenv("CAL_ID", "family")   # ID API du calendrier Famille = "family"
WEEKS         = int(os.getenv("WEEKS", "5"))    # ~1 mois
HEADFUL       = os.getenv("HEADFUL", "0") in ("1","true","True")
CLEAN_ORPHANS = os.getenv("CLEAN_ORPHANS", "0") in ("1","true","True")

TITLE_PREFIX  = "[Mo] "
GCAL_COLOR_ID = "5"  # jaune
TZ            = "Europe/Paris"
TIMEOUT_MS    = 60_000
SCREEN_DIR    = "screenshots"
SRC_TAG       = "PRONOTE_MO"  # marqueur pour cleanup

# ================== GOOGLE CALENDAR ===================

def load_gcal_service():
    if not os.path.exists("token.json"):
        raise SystemExit("[FATAL] token.json manquant (voir étape de génération du token).")
    creds = Credentials.from_authorized_user_file("token.json", scopes=[
        "https://www.googleapis.com/auth/calendar"
    ])
    return build("calendar", "v3", credentials=creds, cache_discovery=False)

def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def make_event_id(summary: str, start_iso: str, end_iso: str, location: str = "") -> str:
    raw = f"{_normalize(summary)}|{start_iso}|{end_iso}|{_normalize(location)}".encode("utf-8")
    digest = hashlib.sha1(raw).digest()
    # id autorisé: [a-z0-9_-], max 1024 – on génère un base32 sûr, tronqué
    b32 = base64.b32encode(digest).decode("ascii").lower().strip("=")
    return "mo-" + b32[:40]

def upsert_events(service, calendar_id: str, events: List[Dict]) -> set:
    created = updated = 0
    pushed_ids = set()
    for ev in events:
        eid = make_event_id(ev["summary"], ev["start"]["dateTime"], ev["end"]["dateTime"], ev.get("location",""))
        body = dict(ev)
        body["colorId"] = GCAL_COLOR_ID
        # tag privé pour cleanup/diagnostic
        priv = body.setdefault("extendedProperties", {}).setdefault("private", {})
        priv.update({"src": SRC_TAG, "key": eid})

        try:
            # insert idempotent: on impose notre ID; 409 => déjà présent
            service.events().insert(calendarId=calendar_id, body={**body, "id": eid}, sendUpdates="none").execute()
            created += 1
        except HttpError as e:
            if getattr(e, "resp", None) and e.resp.status == 409:
                service.events().patch(calendarId=calendar_id, eventId=eid, body=body, sendUpdates="none").execute()
                updated += 1
            else:
                print("[GCAL] Insert error:", e)
        pushed_ids.add(eid)
    print(f"Terminé. créés={created}, maj={updated}")
    return pushed_ids

def cleanup_orphans(service, calendar_id: str, window_start_iso: str, window_end_iso: str, keep_ids: set):
    page = None
    removed = 0
    while True:
        resp = service.events().list(
            calendarId=calendar_id,
            privateExtendedProperty=f"src={SRC_TAG}",
            timeMin=window_start_iso,
            timeMax=window_end_iso,
            singleEvents=True,
            maxResults=2500,
            pageToken=page
        ).execute()
        for it in resp.get("items", []):
            if it.get("id") not in keep_ids:
                try:
                    service.events().delete(calendarId=calendar_id, eventId=it["id"], sendUpdates="none").execute()
                    removed += 1
                except HttpError:
                    pass
        page = resp.get("nextPageToken")
        if not page:
            break
    if removed:
        print(f"[GCAL] Orphelins supprimés: {removed}")

# ================== EXTRACTION PRONOTE =================

# JS exécuté dans la frame d'emploi du temps
JS_EXTRACT_WEEK = r"""
() => {
  const out = { ok:false, weekStart:null, headers:[], items:[] };

  const txt = (document.body.innerText||'').replace(/\u00A0/g,' ').replace(/\s+/g,' ');
  const mh = txt.match(/du\s+(\d{2})\/(\d{2})\/(\d{4})\s+au\s+(\d{2})\/(\d{2})\/(\d{4})/i);
  if (mh) out.weekStart = `${mh[3]}-${mh[2]}-${mh[1]}`;

  // entêtes jours (lun./mar./...)
  const headers = [];
  document.querySelectorAll('div,span,li').forEach(el=>{
    const t=(el.textContent||'').trim();
    if (/^(lun|mar|mer|jeu|ven)\.\s*\d{1,2}\s*(janv|févr|fevr|mars|avr|mai|juin|juil|août|aout|sept|oct|nov|déc|dec)\.?$/i.test(t)) {
      const r=el.getBoundingClientRect();
      if (r.width>20 && r.height>10) headers.push({label:t, left:r.left, right:r.right});
    }
  });
  out.headers = headers.map(h=>h.label);

  const cases = [];
  const re = /(?:^|\s)(De|du)\s*(\d{1,2})[:hH](\d{2})\s*à\s*(\d{1,2})[:hH](\d{2})(?:\s|$)/i;
  document.querySelectorAll('*').forEach(el=>{
    const t=(el.innerText||'').replace(/\u00A0/g,' ').trim();
    if (!re.test(t)) return;
    const r=el.getBoundingClientRect();
    if (r.width<70 || r.height<20) return;
    const m=t.match(re);
    const lines=t.split(/\n+/).map(s=>s.trim()).filter(Boolean);
    const title = (lines.find(s=>!re.test(s)) || '').replace(/\s{2,}/g,' ');
    cases.push({text:t, title, rect:{left:r.left,right:r.right}, times:{sh:m[2],sm:m[3],eh:m[4],em:m[5]}});
  });

  function whichHeader(x){
    let best=null, bestd=1e9;
    for (const h of headers){
      const c=(h.left+h.right)/2;
      const d=Math.abs(c-x);
      if (d<bestd){bestd=d;best=h;}
    }
    return best ? best.label : null;
  }

  out.items = cases.map(c=>({header:whichHeader((c.rect.left+c.rect.right)/2), title:c.title, text:c.text, times:c.times}));
  out.ok = true;
  return out;
}
"""

DAYHEAD_RE = re.compile(r"(?i)\b(lun|mar|mer|jeu|ven)\.\s*(\d{1,2})\s*(janv|févr|fevr|mars|avr|mai|juin|juil|août|aout|sept|oct|nov|déc|dec)\.?")
MONTHS = {"janv":"01","févr":"02","fevr":"02","mars":"03","avr":"04","mai":"05","juin":"06","juil":"07","août":"08","aout":"08","sept":"09","oct":"10","nov":"11","déc":"12","dec":"12"}

def parse_week_payload(payload: dict) -> List[Dict]:
    if not payload.get("ok"):
        return []
    week_start = payload.get("weekStart")
    headers = payload.get("headers") or []
    items = payload.get("items") or []

    # map header -> YYYY-MM-DD
    header_dates = {}
    if week_start:
        # si on a la date du lundi, on peut avancer par index 0..4,
        # mais on ne connaît pas l'index; on reconstruit via label aussi:
        monday = datetime.fromisoformat(week_start).date()
    else:
        monday = None

    # dérive les dates par en-tête texte (plus fiable)
    # on suppose année scolaire courante
    cur = datetime.now()
    for lbl in headers:
        m = DAYHEAD_RE.search(lbl or "")
        if not m:
            continue
        day = int(m.group(2))
        month = MONTHS[m.group(3).lower()]
        year = cur.year
        if cur.month >= 9 and int(month) <= 8:
            year += 1
        header_dates[lbl] = datetime(year, int(month), day).date()

    events = []
    for it in items:
        lbl = it.get("header")
        date = header_dates.get(lbl)
        if not date and monday:
            # fallback si pas d'entête reconnu
            date = monday

        times = it.get("times") or {}
        try:
            sh, sm, eh, em = map(int, (times["sh"], times["sm"], times["eh"], times["em"]))
        except Exception:
            continue

        start = datetime(date.year, date.month, date.day, sh, sm, tzinfo=timezone(timedelta(hours=2)))  # Europe/Paris (été)
        end   = datetime(date.year, date.month, date.day, eh, em, tzinfo=timezone(timedelta(hours=2)))
        title = (it.get("title") or "").strip() or "Cours"
        # nettoyage léger
        title = re.sub(r"\s{2,}", " ", title)
        summary = f"{TITLE_PREFIX}{title}"

        events.append({
            "summary": summary,
            "start": {"dateTime": start.isoformat(), "timeZone": TZ},
            "end":   {"dateTime": end.isoformat(),   "timeZone": TZ},
            "description": it.get("text") or "",
        })

    # dédoublage intra-semaine (même résumé+crénau)
    uniq = {}
    for e in events:
        k = (e["summary"], e["start"]["dateTime"], e["end"]["dateTime"])
        uniq[k] = e
    return list(uniq.values())

def find_timetable_frame(page) -> Optional[object]:
    """Retourne la frame qui contient l’EDT (horaires + 'du jj/mm/aaaa au ...')."""
    js = r"""() => {
      const t = (document.body.innerText||'').replace(/\s+/g,' ');
      const hasRange = /du\s+\d{2}\/\d{2}\/\d{4}\s+au\s+\d{2}\/\d{2}\/\d{4}/i.test(t);
      const hasTimes = /\b(De|du)\s*\d{1,2}[:hH]\d{2}\s*à\s*\d{1,2}[:hH]\d{2}\b/i.test(t);
      return hasTimes || hasRange;
    }"""
    deadline = time.time() + (TIMEOUT_MS/1000)
    while time.time() < deadline:
        for fr in page.frames:
            try:
                if fr.evaluate(js):
                    return fr
            except Exception:
                pass
        page.wait_for_timeout(400)
    return None

def click_next_week_anywhere(page_or_frame) -> bool:
    # essaie dans la frame puis dans la page
    def _try(container):
        selectors = [
            "[title*='semaine suivante']",
            "[aria-label*='semaine suivante']",
            "button:has-text('>')",
            "button[title*='suiv']",
        ]
        for sel in selectors:
            try:
                container.locator(sel).first.click(timeout=800)
                return True
            except Exception:
                pass
        try:
            container.keyboard.press("ArrowRight")
            return True
        except Exception:
            return False

    return _try(page_or_frame) or _try(page_or_frame.page if hasattr(page_or_frame, "page") else page_or_frame)

# ================== NAVIGATION ========================

def login_ent(page):
    page.goto(ENT_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
    # bouton "Se connecter" éventuel
    try:
        page.get_by_role("link", name=re.compile("Se connecter", re.I)).first.click(timeout=3000)
    except Exception:
        pass

    # champs + soumission
    login_sel = 'input[name="username"], input#username, input[name="email"], input[type="email"]'
    pass_sel  = 'input[name="password"], input#password, input[type="password"]'
    page.wait_for_selector(login_sel, timeout=TIMEOUT_MS)
    page.fill(login_sel, PRONOTE_USER)
    page.fill(pass_sel, PRONOTE_PASS)
    for sel in ['button[type="submit"]','input[type="submit"]','button:has-text("Se connecter")','button:has-text("Connexion")']:
        try:
            page.click(sel, timeout=2500); break
        except Exception:
            continue
    page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)

def goto_pronote(page):
    # après login ENT, on ouvre PRONOTE (URL parent.html fournie)
    page.goto(PRONOTE_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
    page.wait_for_timeout(800)

    # cliquer "Vie scolaire" puis "Emploi du temps" si visible
    try:
        page.get_by_text(re.compile(r"Vie scolaire", re.I)).first.click(timeout=3000)
    except Exception:
        pass
    try:
        page.get_by_text(re.compile(r"Emploi du temps", re.I)).first.click(timeout=TIMEOUT_MS)
    except Exception:
        pass

# ================== MAIN ==============================

def main():
    svc = load_gcal_service()

    os.makedirs(SCREEN_DIR, exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not HEADFUL, args=["--lang=fr-FR"])
        ctx = browser.new_context(locale="fr-FR", timezone_id=TZ)
        page = ctx.new_page()
        if HEADFUL:
            page.set_viewport_size({"width": 1400, "height": 900})

        # ENT -> PRONOTE
        login_ent(page)
        goto_pronote(page)

        all_events: List[Dict] = []
        for w in range(WEEKS):
            # trouver la frame EDT
            fr = find_timetable_frame(page)
            if not fr:
                page.screenshot(path=f"{SCREEN_DIR}/fail_no_timetable_w{w+1}.png", full_page=True)
                raise SystemExit("[FATAL] Emploi du temps introuvable dans la page/frames.")

            # extraction de la semaine courante visible
            payload = fr.evaluate(JS_EXTRACT_WEEK)
            week_events = parse_week_payload(payload)
            print(f"Semaine {w+1}: {len(week_events)} évènements")
            all_events.extend(week_events)

            # capture
            try:
                (fr or page).screenshot(path=f"{SCREEN_DIR}/week_{w+1}.png")
            except Exception:
                page.screenshot(path=f"{SCREEN_DIR}/week_{w+1}_page.png", full_page=True)

            # semaine suivante ?
            if w < WEEKS - 1:
                if not click_next_week_anywhere(fr) and not click_next_week_anywhere(page):
                    page.wait_for_timeout(800)

                # petit délai de rendu
                page.wait_for_timeout(900)

        # dédoublage global
        unique = {}
        for e in all_events:
            k = (e["summary"], e["start"]["dateTime"], e["end"]["dateTime"])
            unique[k] = e
        all_events = list(unique.values())

        # upsert
        pushed = upsert_events(svc, CAL_ID, all_events)

        # cleanup orphelins (facultatif)
        if CLEAN_ORPHANS and all_events:
            window_start = min(e["start"]["dateTime"] for e in all_events)
            window_end   = max(e["end"]["dateTime"]   for e in all_events)
            cleanup_orphans(svc, CAL_ID, window_start, window_end, pushed)

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
