# pronote_playwright_to_family_mo.py
import os, re, json, time, hashlib, base64
from datetime import datetime, timedelta
from dateutil.tz import gettz
from dateutil.parser import parse as dtparse

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

TZ = gettz("Europe/Paris")
CAL_ID = os.getenv("CAL_ID", "family")
WEEKS = int(os.getenv("WEEKS", "4"))
HEADFUL = os.getenv("HEADFUL", "0") == "1"

ENT_URL = os.environ["ENT_URL"]
PRONOTE_URL = os.environ["PRONOTE_URL"]
PRONOTE_USER = os.environ["PRONOTE_USER"]
PRONOTE_PASS = os.environ["PRONOTE_PASS"]

# ---------- Utilitaires Google Calendar ----------
def gcal_service():
    with open("token.json","r",encoding="utf-8") as f:
        creds = Credentials.from_authorized_user_info(json.load(f), scopes=[
            "https://www.googleapis.com/auth/calendar.events"
        ])
    return build("calendar","v3",credentials=creds, cache_discovery=False)

def make_event_id(s: str) -> str:
    digest = hashlib.sha1(s.encode("utf-8")).digest()
    return "pmo-" + base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

def upsert_events(service, events):
    seen_ids = set()
    created = updated = 0
    for ev in events:
        eid = make_event_id(
            f"{ev['summary']}|{ev['start']['dateTime']}|{ev['end']['dateTime']}|{ev.get('location','')}"
        )
        seen_ids.add(eid)
        body = dict(ev)
        try:
            service.events().insert(calendarId=CAL_ID, eventId=eid, body=body, sendUpdates="none").execute()
            created += 1
        except HttpError as e:
            if e.resp.status == 409:
                # existe déjà → update pour éviter les doublons s’il y a eu un changement
                service.events().update(calendarId=CAL_ID, eventId=eid, body=body, sendUpdates="none").execute()
                updated += 1
            else:
                print("[GCAL] Insert error:", e)

    print(f"Terminé. créés={created}, maj={updated}")
    return seen_ids

# ---------- Extraction côté PRONOTE (via JS dans la page) ----------
JS_EXTRACT = r"""
() => {
  // Renvoie: { weekStart: 'YYYY-MM-DD', entries: [ {left,x,y,width,height,text} ... ] }
  const out = { weekStart: null, entries: [] };

  // 1) récupérer la date de début de semaine dans le bandeau "du jj/mm/aaaa au ..."
  const hdr = Array.from(document.querySelectorAll("*"))
    .map(e => e.textContent?.trim() || "")
    .find(t => /du\s+\d{2}\/\d{2}\/\d{4}\s+au\s+\d{2}\/\d{2}\/\d{4}/i.test(t));

  if (hdr) {
    const m = hdr.match(/du\s+(\d{2})\/(\d{2})\/(\d{4})\s+au/i);
    if (m) {
      out.weekStart = `${m[3]}-${m[2]}-${m[1]}`; // YYYY-MM-DD
    }
  }

  // 2) repérer les colonnes des jours via les entêtes "lun. mar. ..."
  function dayColumns() {
    // On prend les titres de colonnes (lun./mar./...) visibles
    const labels = ['lun','mar','mer','jeu','ven','sam','dim'];
    const nodes = [];
    document.querySelectorAll('*').forEach(e=>{
      const t=(e.textContent||'').toLowerCase();
      if (/(lun\.|mar\.|mer\.|jeu\.|ven\.|sam\.|dim\.)/.test(t)) {
        const r=e.getBoundingClientRect();
        if (r.width>30 && r.height<80) nodes.push({x:r.left, y:r.top, el:e});
      }
    });
    // regrouper par x ~ même colonne
    nodes.sort((a,b)=>a.x-b.x);
    const cols=[];
    for (const n of nodes) {
      if (!cols.length || Math.abs(cols[cols.length-1]-n.x)>40) cols.push(n.x);
    }
    return cols;
  }

  const cols = dayColumns();

  // 3) repérer les "cases" de cours par motif "De HH:MM à HH:MM" (ou HHhMM)
  const timeRe = /De\s*(\d{1,2})[:hH]?(\d{2})\s*à\s*(\d{1,2})[:hH]?(\d{2})/;
  const all = Array.from(document.querySelectorAll("div,li,span"));

  function closestDay(x) {
    if (!cols.length) return 0;
    let best = 0, bd = 1e9;
    cols.forEach((cx, i)=>{
      const d=Math.abs(cx-x);
      if (d<bd){bd=d;best=i}
    });
    return best; // index 0=Monday
  }

  for (const e of all) {
    const t = (e.innerText || "").replace(/\u00A0/g,' ').trim();
    if (!timeRe.test(t)) continue;
    const r = e.getBoundingClientRect();
    if (r.width < 80 || r.height < 20) continue; // éviter le bruit
    const m = t.match(timeRe);
    // titre = première ligne non "De ... à ..."
    const lines = t.split(/\n+/).map(s=>s.trim()).filter(Boolean);
    const title = (lines.find(s=>!timeRe.test(s)) || "").replace(/\s{2,}/g,' ');
    out.entries.push({
      text: t,
      title,
      rect: {left:r.left, top:r.top, width:r.width, height:r.height},
      colIndex: closestDay(r.left),
      times: { sh:m[1], sm:m[2], eh:m[3], em:m[4] }
    });
  }

  return out;
}
"""

def _to_dt(base_date: datetime, h: int, m: int) -> datetime:
    return datetime(base_date.year, base_date.month, base_date.day, h, m, tzinfo=TZ)

def parse_to_events(week_start: str, entries: list):
    """Transforme les 'entries' JS en événements Calendar."""
    monday = dtparse(week_start).replace(tzinfo=TZ)
    events = []
    for e in entries:
        day = monday + timedelta(days=int(e["colIndex"]))
        sh, sm = int(e["times"]["sh"]), int(e["times"]["sm"])
        eh, em = int(e["times"]["eh"]), int(e["times"]["em"])
        start = _to_dt(day, sh, sm)
        end   = _to_dt(day, eh, em)

        title = e["title"] or "Cours"
        # Nettoyage: supprimer mentions type "[4G_ESP2]" entre crochets dans le titre
        title = re.sub(r"\[[^\]]+\]", "", title).strip()
        # Essayons d'extraire une salle s'il y a un code style "S01", "017 Permanence", etc.
        loc = None
        m = re.search(r"\b(S\d{2}|\d{3}\s+Permanence|\bSalle\s+[A-Z0-9]+)\b", e["text"], flags=re.I)
        if m: loc = m.group(0)

        events.append({
            "summary": title,
            "location": loc,
            "start": {"dateTime": start.isoformat()},
            "end":   {"dateTime": end.isoformat()},
            "transparency": "opaque",
            "source": {"title":"PRONOTE", "url": PRONOTE_URL}
        })
    return events

# ---------- Navigation ENT → PRONOTE → Emploi du temps ----------
def login_ent(page):
    page.goto(ENT_URL, wait_until="domcontentloaded", timeout=60000)

    # Bouton "Se connecter" (si page d'accueil ENT)
    try:
        page.get_by_role("link", name=re.compile("Se connecter", re.I)).first.click(timeout=6000)
    except Exception:
        pass

    # Champs usuels
    login_sel = 'input[name="username"], input#username, input[name="email"], input[type="email"]'
    pass_sel  = 'input[name="password"], input#password, input[type="password"]'
    page.wait_for_selector(login_sel, timeout=60000)
    page.fill(login_sel, PRONOTE_USER)
    page.fill(pass_sel, PRONOTE_PASS)
    # Bouton submit : "Se connecter" / "Connexion" / bouton type submit
    for sel in [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Se connecter")',
        'button:has-text("Connexion")',
    ]:
        try:
            page.click(sel, timeout=4000)
            break
        except Exception:
            continue

    # L’ENT peut rediriger ou rester ; on force ensuite la page PRONOTE
    page.wait_for_load_state("domcontentloaded", timeout=45000)

def goto_pronote_timetable(page):
    # On va directement sur l’URL PRONOTE une fois loggé ENT
    page.goto(PRONOTE_URL, wait_until="domcontentloaded", timeout=60000)

    # Assurer qu’on est bien dans l’appli PRONOTE
    # Puis attendre que le mot "Emploi du temps" soit visible
    page.wait_for_timeout(1000)
    # si menu latéral, cliquer "Vie scolaire" > "Emploi du temps"
    try:
        page.get_by_text(re.compile(r"Emploi du temps", re.I)).first.click(timeout=5000)
    except Exception:
        pass

    # on attend que les cases horaires soient rendues
    # motif "De HH:MM à HH:MM" quelque part sur la page
    page.wait_for_function(
        "()=>/De\\s*\\d{1,2}[:hH]?\\d{2}\\s*à\\s*\\d{1,2}[:hH]?\\d{2}/.test(document.body.innerText)",
        timeout=60000
    )

def click_next_week(page):
    # Plusieurs PRONOTE ont un bouton ">" ou "semaine suivante".
    # On tente diverses cibles, sinon on simule le raccourci clavier (flèche droite).
    candidates = [
        'button[title*="suiv"]',
        'button[aria-label*="suiv"]',
        'a[title*="suiv"]',
        'a[aria-label*="suiv"]',
        'button:has-text(">")'
    ]
    for sel in candidates:
        try:
            page.click(sel, timeout=1200)
            return True
        except Exception:
            continue
    try:
        page.keyboard.press("ArrowRight")
        return True
    except Exception:
        return False

def main():
    # Init GCal
    service = gcal_service()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not HEADFUL, args=["--lang=fr-FR"])
        ctx = browser.new_context(locale="fr-FR")
        page = ctx.new_page()

        # Aide visuelle en mode headful
        if HEADFUL:
            page.set_viewport_size({"width": 1420, "height": 900})

        # 1) ENT login
        login_ent(page)

        # 2) PRONOTE → Emploi du temps
        goto_pronote_timetable(page)

        all_events = []
        for w in range(WEEKS):
            try:
                data = page.evaluate(JS_EXTRACT)
            except PWTimeout:
                raise SystemExit("[FATAL] Timeout lors de l’extraction.")
            weekStart = data.get("weekStart")
            entries = data.get("entries", [])
            print(f"Semaine {w+1}: {len(entries)} cases, weekStart={weekStart!r}")

            if weekStart and entries:
                evs = parse_to_events(weekStart, entries)
                all_events.extend(evs)

            # Screenshot (debug)
            try:
                page.screenshot(path=f"screenshots/sem_{w+1}.png", full_page=True)
            except Exception:
                pass

            if w < WEEKS-1:
                ok = click_next_week(page)
                if not ok:
                    # si le bouton semaine suivante n’existe pas, on tente d’avancer via calendrier
                    page.wait_for_timeout(800)
                page.wait_for_timeout(900)  # laisser le temps au rendu

        # Dédup sur la session (au cas où une case a été vue deux fois)
        uniq = {}
        for e in all_events:
            k = (e["summary"], e["start"]["dateTime"], e["end"]["dateTime"], e.get("location"))
            uniq[k] = e
        all_events = list(uniq.values())

        # 3) Upsert dans Google Calendar (agenda Famille)
        upsert_events(service, all_events)

        ctx.close()
        browser.close()

if __name__ == "__main__":
    main()
