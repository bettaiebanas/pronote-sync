
# pronote_playwright_to_family_mo.py (robust week-tab finder)
from __future__ import annotations

import os, re, sys, time, json, hashlib, unicodedata
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Union, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Frame
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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
WEEKS_TO_FETCH         = int(os.getenv("WEEKS_TO_FETCH", "44"))
CLICK_TOUT_VOIR        = os.getenv("CLICK_TOUT_VOIR", "1") == "1"
WAIT_AFTER_NAV_MS      = int(os.getenv("WAIT_AFTER_NAV_MS", "1000"))
MAX_TILES_PER_WEEK     = int(os.getenv("MAX_TILES_PER_WEEK", "140"))
MAX_CLICK_PER_SELECTOR = int(os.getenv("MAX_CLICK_PER_SELECTOR", "140"))
WEEK_HARD_TIMEOUT_MS   = int(os.getenv("WEEK_HARD_TIMEOUT_MS", "120000"))
PANEL_WAIT_MS          = int(os.getenv("PANEL_WAIT_MS", "350"))
PANEL_RETRIES          = int(os.getenv("PANEL_RETRIES", "8"))
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
SCOPES           = ["https://www.googleapis.com/auth/calendar"]
TIMEZONE         = "Europe/Paris"
TIMEOUT_MS       = 120_000
SCREEN_DIR       = "screenshots"

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); print(f"[{ts}] {msg}")

def _safe_mkdir(p: str) -> None:
    try: os.makedirs(p, exist_ok=True)
    except Exception: pass

def _safe_shot(page_or_frame: Union[Page, Frame], name: str) -> None:
    try:
        page = page_or_frame if isinstance(page_or_frame, Page) else page_or_frame.page
        _safe_mkdir(SCREEN_DIR); page.screenshot(path=f"{SCREEN_DIR}/{name}.png", full_page=True)
    except Exception: pass

def _safe_write(path: str, data: str, enc: str = "utf-8") -> None:
    try:
        _safe_mkdir(os.path.dirname(path) or ".")
        with open(path, "w", encoding=enc) as f: f.write(data)
    except Exception: pass

# -------------- GCAL helpers (unchanged logic for brevity) --------------
def get_gcal_service():
    if not CALENDAR_ID: raise SystemExit("CALENDAR_ID manquant.")
    from google.auth.transport.requests import Request
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        creds = None
    if not creds or not creds.valid:
        if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as f: f.write(creds.to_json())
    return build("calendar", "v3", credentials=creds)

def _norm(s: str) -> str:
    return re.sub(r"\s+"," ",unicodedata.normalize("NFKD", s or "").encode("ascii","ignore").decode()).strip().lower()

def dedupe_base(summary_raw: str, room: str) -> str:
    s = re.sub(r'^\s*\[[^\]]+\]\s*', '', summary_raw or '')
    s = re.sub(r'\s*\((prof\.?\s*absent|cours\s+annul[eé]|changement\s+de\s+salle|cours\s+modifi[eé])\)\s*$', '', s, flags=re.I)
    return f"{_norm(s)}|{_norm(room or '')}"

def make_dedupe_key(start: datetime, end: datetime, summary_raw: str, room: str) -> str:
    import hashlib
    base = dedupe_base(summary_raw, room)
    return hashlib.sha1(f"{start.isoformat()}|{end.isoformat()}|{base}".encode()).hexdigest()

def parse_iso(dt_str: str) -> Optional[datetime]:
    if not dt_str: return None
    dt_str = dt_str.replace('Z','+00:00')
    try: return datetime.fromisoformat(dt_str)
    except Exception: return None

def upsert_event_by_dedupe(svc, cal_id: str, body: Dict[str, Any], summary_raw: str, room: str):
    s_dt = datetime.fromisoformat(body["start"]["dateTime"]); e_dt = datetime.fromisoformat(body["end"]["dateTime"])
    dedupe_key = make_dedupe_key(s_dt, e_dt, summary_raw, room)
    try:
        q = svc.events().list(calendarId=cal_id, privateExtendedProperty=f"dedupe={dedupe_key}",
                              timeMin=body["start"]["dateTime"], timeMax=(e_dt+timedelta(minutes=1)).isoformat(),
                              singleEvents=True, showDeleted=False, maxResults=2).execute()
        items = q.get("items", [])
    except HttpError: items = []
    if not items:
        try:
            w = svc.events().list(calendarId=cal_id, timeMin=body["start"]["dateTime"], timeMax=(e_dt+timedelta(minutes=1)).isoformat(),
                                  singleEvents=True, showDeleted=False, maxResults=50, privateExtendedProperty="source=pronote_playwright").execute()
            for ev in w.get("items", []):
                s0 = parse_iso(ev.get("start",{}).get("dateTime")); e0 = parse_iso(ev.get("end",{}).get("dateTime"))
                if not s0 or not e0: continue
                if s0.isoformat()!=body["start"]["dateTime"] or e0.isoformat()!=body["end"]["dateTime"]: continue
                if dedupe_base(ev.get("summary",""), ev.get("location","")) == dedupe_base(summary_raw, room):
                    body.setdefault("extendedProperties", {}).setdefault("private", {})["dedupe"] = dedupe_key
                    ev = svc.events().patch(calendarId=cal_id, eventId=ev["id"], body=body, sendUpdates="none").execute()
                    return "updated", ev
        except HttpError: pass
    if items:
        ev = svc.events().patch(calendarId=cal_id, eventId=items[0]["id"], body=body, sendUpdates="none").execute()
        return "updated", ev
    ev = svc.events().insert(calendarId=cal_id, body=body, sendUpdates="none").execute()
    return "created", ev

# -------------- PRONOTE parsing helpers --------------
H_PATTERNS = [re.compile(r'(?P<h>\d{1,2})\s*[hH:]\s*(?P<m>\d{2})'),
              re.compile(r'(?P<h>\d{1,2})\s*(?:heures?|hrs?)\s*(?P<m>\d{2})', re.I)]
DURATION_RE = re.compile(r'(?P<dh>\d{1,2})\s*[hH]\s*(?P<dm>\d{2})')
MONTHS_FR = {"janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,"mai":5,"juin":6,"juillet":7,"août":8,"aout":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,"decembre":12}

def parse_times(t: str) -> Dict[str, Optional[tuple[int,int]]]:
    t = (t or "").strip(); hours=[]
    for rx in H_PATTERNS:
        for m in rx.finditer(t): hours.append((int(m.group("h")), int(m.group("m"))))
    if 'à' in t and re.search(r'\d{1,2}\s*/\s*\d{1,2}(?:\s*/\s*\d{2,4})?', t) and len(hours)==2:
        return {"start": hours[1], "end": None, "duration": hours[0]}
    if len(hours)>=2: return {"start":hours[0],"end":hours[1],"duration":None}
    dm = DURATION_RE.search(t)
    if dm and hours: return {"start":hours[-1],"end":None,"duration":(int(dm.group("dh")),int(dm.group("dm")))}
    return {"start":None,"end":None,"duration":None}

def parse_date_from_text(text: str, fallback_year: int) -> Optional[datetime]:
    m = re.search(r'(\d{1,2})\s*/\s*(\d{1,2})(?:\s*/\s*(\d{2,4}))?', text or '')
    if m:
        d, mo = int(m.group(1)), int(m.group(2)); y = int(m.group(3)) if m.group(3) else fallback_year
        if y<100: y+=2000; return datetime(y, mo, d)
    m = re.search(r'(\d{1,2})\s+([A-Za-zéûùôîïàâç]+)', text or '', re.I)
    if m:
        d = int(m.group(1)); mo = MONTHS_FR.get(m.group(2).lower())
        if mo: return datetime(fallback_year, mo, d)
    return None

def to_dt(d: datetime, hm: tuple[int,int]) -> datetime:
    return d.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)

def parse_panel(panel: Dict[str, Any], year: int) -> Optional[Dict[str, Any]]:
    header = panel.get("header",""); mat = re.sub(r'\s+',' ',(panel.get("matiere","") or "").strip()); salle = re.sub(r'\s+',' ',(panel.get("salle","") or "").strip())
    times = parse_times(header); 
    if not (times["start"] or times["end"]): return None
    dt_date = parse_date_from_text(header, fallback_year=year); 
    if not dt_date: return None
    start_hm = times["start"]; end_hm = times["end"]
    if start_hm and end_hm: start_dt, end_dt = to_dt(dt_date,start_hm), to_dt(dt_date,end_hm)
    elif start_hm and times["duration"]: dh,dm=times["duration"]; start_dt=to_dt(dt_date,start_hm); end_dt=start_dt+timedelta(hours=dh,minutes=dm)
    else: return None
    if not salle:
        m = re.search(r'(?:Salle[s]?\s+)(.+)$', header, re.I)
        if m: salle = m.group(1).strip()
    return {"summary": mat or "Cours","room":salle,"start_dt":start_dt,"end_dt":end_dt}

# -------------- Playwright helpers --------------
def accept_cookies_any(page: Page) -> None:
    texts = ["Tout accepter","Accepter tout","J'accepte","Accepter","OK","Continuer","J'ai compris"]
    sels = [f'button:has-text("{t}")' for t in texts] + [f'role=button[name="{t}"]' for t in texts]
    for s in sels:
        try:
            if page.locator(s).count()>0:
                page.locator(s).first.click(); break
        except Exception: pass

def find_timetable_ctx(page: Page, timeout_ms: int = TIMEOUT_MS) -> Union[Page, Frame]:
    js = r"""() => { const txt=(document.body.innerText||'').replace(/\s+/g,' ');
      const hasTitle=/Emploi du temps/i.test(txt) || /Planning|Agenda/i.test(txt);
      const hasWeek=/(Semaine|du\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\s+au\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)/i.test(txt);
      const hasTimes=/\d{1,2}\s*[h:]\s*\d{2}|heures?\s*\d{2}/i.test(txt);
      return (hasTitle&&(hasTimes||hasWeek))||(hasWeek&&hasTimes);
    }"""
    deadline = time.time()+timeout_ms/1000.0
    while time.time()<deadline:
        try:
            for fr in page.frames:
                if fr.evaluate(js): return fr
            if page.evaluate(js): return page
        except Exception: pass
        page.wait_for_timeout(200)
    return page

def find_dom_grid_ctx(page: Page, prefer: Optional[Union[Page, Frame]] = None, timeout_ms: int = 5000) -> Optional[Union[Page, Frame]]:
    end=time.time()+timeout_ms/1000.0
    js = r"""()=>{const q1=document.querySelectorAll('[id^="id_"][id*="_coursInt_"]').length;
      const q2=document.querySelectorAll('[id^="id_"][id*="_cont"]').length;
      const q3=document.querySelectorAll('.EnteteCoursLibelle').length; return (q1+q2+q3)>0;}"""
    while time.time()<end:
        cand=[]; 
        if prefer: cand.append(prefer)
        cand.extend(list(page.frames)); cand.append(page)
        for ctx in cand:
            try:
                if ctx.evaluate(js): return ctx
            except Exception: pass
        page.wait_for_timeout(200)
    return None

def goto_timetable(pronote_page: Page) -> Union[Page, Frame]:
    accept_cookies_any(pronote_page)
    if TIMETABLE_PRE_SELECTOR:
        try: pronote_page.locator(TIMETABLE_PRE_SELECTOR).first.click(); pronote_page.wait_for_timeout(WAIT_AFTER_NAV_MS)
        except Exception: pass
    if TIMETABLE_SELECTOR:
        try: pronote_page.locator(TIMETABLE_SELECTOR).first.click(); pronote_page.wait_for_timeout(WAIT_AFTER_NAV_MS)
        except Exception: pass
    ctx=find_timetable_ctx(pronote_page, timeout_ms=10_000); grid=find_dom_grid_ctx(pronote_page, prefer=ctx, timeout_ms=5000) or ctx
    return grid

def _list_course_ids(ctx: Union[Page, Frame]) -> List[str]:
    try:
        return ctx.evaluate(r"""() => {
          const pos = (e)=> (e?.getBoundingClientRect()?.top || 9e9);
          const uniq={}; for (const e of document.querySelectorAll('[id^="id_"][id*="_coursInt_"]')) uniq[e.id]=pos(e);
          for (const e of document.querySelectorAll('[id^="id_"][id*="_cont"]')) { const t=pos(e); uniq[e.id]=Math.min(uniq[e.id]??t,t); }
          return Object.keys(uniq).sort((a,b)=>uniq[a]-uniq[b]);
        }""") or []
    except Exception: return []

def _click_by_id(ctx: Union[Page, Frame], el_id: str) -> bool:
    try:
        return bool(ctx.evaluate("""(id)=>{ const el=document.getElementById(id); if(!el) return false;
          el.scrollIntoView({block:'center'}); try{el.click()}catch(e){};
          try{ el.dispatchEvent(new MouseEvent('mousedown',{bubbles:true}));
               el.dispatchEvent(new MouseEvent('mouseup',{bubbles:true}));
               el.dispatchEvent(new MouseEvent('click',{bubbles:true})); }catch(e){}; return true; }""", el_id))
    except Exception: return False

def _get_tile_status(ctx: Union[Page, Frame], el_id: str) -> str:
    try:
        return ctx.evaluate(r"""(id)=>{ const el=document.getElementById(id); if(!el) return '';
          const t=(el.innerText||'').replace(/\s+/g,' ').trim().toLowerCase();
          const L=['prof. absent','prof absent','cours annulé','cours annule','changement de salle','cours modifié','cours modifie'];
          return L.find(x=>t.includes(x))||'';}""", el_id) or ""
    except Exception: return ""

def _read_visible_panel(ctx: Union[Page, Frame]) -> Optional[Dict[str, Any]]:
    for _ in range(PANEL_RETRIES):
        try:
            panel = ctx.evaluate(r"""()=>{ const P=Array.from(document.querySelectorAll('.ConteneurCours'));
               if(!P.length) return null; const p=P[P.length-1];
               const header=(p.querySelector('.EnteteCoursLibelle')?.innerText||'').replace(/\s+/g,' ').trim();
               const groups=Array.from(p.querySelectorAll('[role="group"]'));
               const pick=(name)=>{ const g=groups.find(x=>(x.getAttribute('aria-label')||'').toLowerCase().includes(name));
                 return g? (g.innerText||'').replace(/\s+/g,' ').trim() : ''; };
               return header? {header, matiere:pick('matière')||pick('matiere'), salle:pick('salles')||pick('salle')} : null; }""")
            if panel: return panel
        except Exception: pass
        time.sleep(PANEL_WAIT_MS/1000.0)
    return None

def _find_and_click_week_any(pronote_page: Page, n: int) -> Optional[Union[Page, Frame]]:
    # 1) tentative via WEEK_TAB_TEMPLATE exact
    css = WEEK_TAB_TEMPLATE.format(n=n) if WEEK_TAB_TEMPLATE else ""
    if css:
        for ctx in list(pronote_page.frames)+[pronote_page]:
            try:
                if ctx.evaluate("(s)=>!!document.querySelector(s)", css):
                    try: ctx.locator(css).first.click()
                    except Exception: ctx.evaluate("(s)=>document.querySelector(s)?.click()", css)
                    (ctx.page if isinstance(ctx, Frame) else ctx).wait_for_timeout(WAIT_AFTER_NAV_MS)
                    grid = find_dom_grid_ctx(pronote_page, prefer=ctx, timeout_ms=5000) or ctx
                    return grid
            except Exception: pass
    # 2) fallback robuste : chercher par REGEX d'id qui finit par _j_n
    pattern = re.compile(rf'^GInterface\.Instances\[\d+\]\.Instances\[\d+\]_j_{n}$')
    for ctx in list(pronote_page.frames)+[pronote_page]:
        try:
            found = ctx.evaluate("""(suffix)=>{
                const out=[]; const rx=new RegExp('^GInterface\\.Instances\\\\[\\\\d+\\\\]\\.Instances\\\\[\\\\d+\\\\]_j_'+suffix+'$');
                for (const el of document.querySelectorAll('[id]')){
                    const id=el.id||'';
                    if (rx.test(id) && (el.getAttribute('role')==='radio' || (el.className||'').includes('calendrier-jour'))) { out.push(id); }
                }
                return out;
            }""", n)
            if found:
                el_id = found[0]
                try: ctx.evaluate("(id)=>document.getElementById(id)?.click()", el_id)
                except Exception: pass
                (ctx.page if isinstance(ctx, Frame) else ctx).wait_for_timeout(WAIT_AFTER_NAV_MS)
                grid = find_dom_grid_ctx(pronote_page, prefer=ctx, timeout_ms=5000) or ctx
                return grid
        except Exception: pass
    return None

def extract_week_info(ctx: Union[Page, Frame]) -> Dict[str, Any]:
    try:
        header_text = ctx.evaluate(r"""()=>{ const txt=(document.body.innerText||'').replace(/\s+/g,' ');
          const m=txt.match(/du\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\s+au\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?/i); return m?m[0]:'';}""")
    except Exception: header_text=""
    monday=None
    try:
        m=re.search(r'du\s+(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?', header_text or '', flags=re.I)
        if m:
            y=int(m.group(3)) if m.group(3) else datetime.now().year
            if y<100: y+=2000
            monday=datetime(y,int(m.group(2)),int(m.group(1)))
    except Exception: monday=None

    tiles=[]; year=(monday.year if monday else datetime.now().year)
    ids=_list_course_ids(ctx); lim=min(len(ids), MAX_TILES_PER_WEEK)
    for i in range(lim):
        el_id=ids[i]; status=_get_tile_status(ctx, el_id)
        if not _click_by_id(ctx, el_id): continue
        panel=_read_visible_panel(ctx); 
        if not panel: continue
        parsed=parse_panel(panel, year)
        if not parsed: continue
        tiles.append({"summary":parsed["summary"],"status":status,"room":parsed["room"],
                      "start_dt":parsed["start_dt"],"end_dt":parsed["end_dt"]})
        try: ctx.evaluate("()=>document.body.click()")
        except Exception: pass
        if len(tiles)>=MAX_TILES_PER_WEEK: break
    return {"monday":monday,"tiles":tiles,"header":header_text}

def run():
    if not ENT_USER or not ENT_PASS: raise SystemExit("PRONOTE_USER / PRONOTE_PASS manquants.")
    svc=get_gcal_service()
    try:
        me=svc.calendars().get(calendarId="primary").execute(); cal=svc.calendars().get(calendarId=CALENDAR_ID).execute()
        log(f"[GCAL] Utilisation agenda '{cal.get('summary','?')}' (id={CALENDAR_ID}) en tant que {me.get('id','?')}")
    except Exception: pass

    created=updated=0
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=not HEADFUL, args=["--disable-dev-shm-usage"])
        context=p.new_context(locale="fr-FR", timezone_id=TIMEZONE)
        page=context.new_page(); page.set_default_timeout(TIMEOUT_MS)

        log("Connexion ENT...")
        page.goto(ENT_URL); page.wait_for_load_state("load"); page.wait_for_load_state("domcontentloaded"); accept_cookies_any(page)
        # login form heuristics (inchangés par rapport à ta version précédente)
        user = None
        for s in ['a:has-text("Se connecter")','a:has-text("Connexion")','button:has-text("Se connecter")','button:has-text("Connexion")','a[href*="login"]','a[href*="auth"]']:
            try:
                if page.locator(s).count(): page.locator(s).first.click(); break
            except Exception: pass
        page.wait_for_load_state("domcontentloaded"); accept_cookies_any(page)
        for s in ['input[name="email"]','input[name="username"]','#username','input[type="text"][name*="user"]','input[type="text"]','input[type="email"]','input#email','input[name="login"]','input[name="j_username"]']:
            if page.locator(s).count(): user=page.locator(s).first; break
        pwd=None
        for s in ['input[type="password"][name="password"]','#password','input[type="password"]','input[name="j_password"]']:
            if page.locator(s).count(): pwd=page.locator(s).first; break
        if not user or not pwd:
            for s in ['button:has-text("Identifiant")','a:has-text("Identifiant")','button:has-text("Compte")','a:has-text("Compte")','a:has-text("ENT")']:
                try:
                    if page.locator(s).count(): page.locator(s).first.click(); page.wait_for_load_state("domcontentloaded"); accept_cookies_any(page); break
                except Exception: pass
            for s in ['input[name="email"]','input[name="username"]','#username','input[type="text"][name*="user"]','input[type="text"]','input[type="email"]','input#email','input[name="login"]','input[name="j_username"]']:
                if page.locator(s).count(): user=page.locator(s).first; break
            for s in ['input[type="password"][name="password"]','#password','input[type="password"]','input[name="j_password"]']:
                if page.locator(s).count(): pwd=page.locator(s).first; break
        if not user or not pwd: raise RuntimeError("Champ identifiant ENT introuvable.")
        user.fill(ENT_USER); pwd.fill(ENT_PASS)
        # submit
        if page.locator('button[type="submit"]').count(): page.locator('button[type="submit"]').first.click()
        elif page.locator('input[type="submit"]').count(): page.locator('input[type="submit"]').first.click()
        else: user.press("Enter")
        page.wait_for_load_state("domcontentloaded"); accept_cookies_any(page)

        log("Ouverture PRONOTE...")
        if PRONOTE_URL:
            page.goto(PRONOTE_URL); page.wait_for_load_state("load"); page.wait_for_load_state("domcontentloaded"); accept_cookies_any(page)
            pronote_page=page
        else:
            with page.expect_popup() as ppu:
                for s in ['a:has-text("PRONOTE")','a[title*="PRONOTE"]','a[href*="pronote"]','text=PRONOTE']:
                    try:
                        if page.locator(s).count(): page.locator(s).first.click(); break
                    except Exception: pass
            try: pronote_page=ppu.value; pronote_page.wait_for_load_state("domcontentloaded")
            except PWTimeout: pronote_page=page; pronote_page.wait_for_load_state("domcontentloaded")
            accept_cookies_any(pronote_page)

        log("Navigation vers 'Emploi du temps'...")
        ctx=goto_timetable(pronote_page)

        start_idx=max(1, FETCH_WEEKS_TO_FETCH:=FETCH_WEEKS_FROM)
        end_idx=start_idx + max(1, WEEKS_TO_FETCH) - 1
        end_idx=min(end_idx, 60)

        for week_idx in range(FETCH_WEEKS_FROM, end_idx + 1):
            log(f"-> Selection Semaine index={week_idx} via css '{WEEK_TAB_TEMPLATE.format(n=week_idx)}'")
            new_ctx=_find_and_click_week_any(pronote_page, week_idx)
            if not new_ctx:
                log(f"[WEEK] Onglet {week_idx} introuvable — on passe.")
                continue
            ctx=new_ctx
            if CLICK_TOUT_VOIR:
                try:
                    for s in ['*:has-text("Tout voir")','*:has-text("Voir tout")','*:has-text("Tout afficher")']:
                        if ctx.locator(s).count(): ctx.locator(s).first.click(); break
                except Exception: pass

            info=extract_week_info(ctx); tiles=info["tiles"] or []; hdr=(info.get("header") or "").replace("\\n"," ")[:160]
            log(f"Semaine {week_idx}: {len(tiles)} cases, header='{hdr}'")
            now=datetime.now()
            for t in tiles:
                start_dt=t["start_dt"]; end_dt=t["end_dt"]; summary=t.get("summary") or "Cours"; room=t.get("room",""); status=(t.get("status") or "")
                if end_dt < (now - timedelta(days=120)) or start_dt > (now + timedelta(days=240)): continue
                tag=""
                if status:
                    s=status
                    if "prof" in s: tag=" (Prof. absent)"
                    elif "annul" in s: tag=" (Cours annulé)"
                    elif "changement de salle" in s: tag=" (Changement de salle)"
                    elif "modifi" in s: tag=" (Cours modifié)"
                title=f"{TITLE_PREFIX}{summary}{tag}"
                body={"summary":title,"location":room,
                      "start":{"dateTime":start_dt.isoformat(),"timeZone":TIMEZONE},
                      "end":{"dateTime":end_dt.isoformat(),"timeZone":TIMEZONE},
                      "colorId":COLOR_ID,
                      "extendedProperties":{"private":{"source":"pronote_playwright"}}}
                try:
                    action,_=upsert_event_by_dedupe(svc, CALENDAR_ID, body, summary, room)
                    if action=="created": created+=1
                    else: updated+=1
                except HttpError as e: log(f"[GCAL] {e}")
        browser.close()
    log(f"Termine. crees={created}, maj={updated}")

if __name__=="__main__":
    try: run()
    except Exception as ex:
        _safe_mkdir(SCREEN_DIR); log(f"[FATAL] {ex}"); sys.exit(1)
