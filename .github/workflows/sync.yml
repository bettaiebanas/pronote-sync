# -*- coding: utf-8 -*-
"""
sync.py — orchestrateur pour pronote_playwright_to_family_mo.py

- Crée credentials.json / token.json depuis les ENV si fournis
- (Re)met pip en place si absent, installe dépendances Python
- Installe les navigateurs Playwright (Chromium)
- Lance la synchro (import et appel direct de run())
"""

import os, sys, json, subprocess, shutil, textwrap, traceback
from pathlib import Path

# ---- Config basique
ROOT = Path(__file__).resolve().parent
PY   = sys.executable
SCREEN_DIR = ROOT / "screenshots"

ENV_PRINT_KEYS = [
    "ENT_URL", "PRONOTE_URL", "CALENDAR_ID", "HEADFUL",
    "TIMETABLE_PRE_SELECTOR", "TIMETABLE_SELECTOR", "TIMETABLE_FRAME",
    "WEEK_TAB_TEMPLATE", "FETCH_WEEKS_FROM", "WEEKS_TO_FETCH",
    "WAIT_AFTER_NAV_MS", "CLICK_TOUT_VOIR", "DEBUG"
]

REQS = [
    "playwright",
    "google-api-python-client",
    "google-auth-httplib2",
    "google-auth-oauthlib",
    "python-dateutil",
]

def log(msg: str):
    print(msg, flush=True)

def run_cmd(args, check=True):
    log(f"[CMD] {' '.join(args)}")
    return subprocess.run(args, check=check)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def write_secret_file(filename: str, env_var: str):
    val = os.getenv(env_var, "")
    if not val:
        return False
    try:
        (ROOT / filename).write_text(val, encoding="utf-8")
        log(f"[OK] {filename} écrit depuis ${env_var}")
        return True
    except Exception as e:
        log(f"[WARN] impossible d’écrire {filename}: {e}")
        return False

def ensure_pip():
    # certains toolcache windows n’ont pas pip dispo par défaut
    try:
        run_cmd([PY, "-m", "pip", "--version"], check=True)
        log("[OK] pip déjà présent")
        return
    except Exception:
        log("[INFO] pip absent — tentative ensurepip")
    try:
        run_cmd([PY, "-m", "ensurepip", "--upgrade"], check=True)
        run_cmd([PY, "-m", "pip", "install", "-U", "pip", "setuptools", "wheel"], check=True)
        log("[OK] pip (ré)installé")
    except Exception as e:
        log(f"[WARN] échec ensurepip/pip upgrade: {e}")

def install_requirements():
    for pkg in REQS:
        try:
            run_cmd([PY, "-m", "pip", "install", "-U", pkg], check=True)
        except subprocess.CalledProcessError as e:
            log(f"[ERR] pip install {pkg} a échoué: rc={e.returncode}")
            raise

def ensure_playwright_browsers():
    # Evite de dépendre du PATH : toujours via "python -m playwright"
    try:
        run_cmd([PY, "-m", "playwright", "install", "chromium"], check=True)
        log("[OK] Playwright Chromium installé")
    except subprocess.CalledProcessError as e:
        log(f"[ERR] playwright install chromium a échoué (rc={e.returncode})")
        raise

def print_env_overview():
    log("[ENV] variables utiles :")
    for k in ENV_PRINT_KEYS:
        v = os.getenv(k)
        if v is None:
            log(f"  - {k}=<absent>")
        else:
            show = v if k not in ("PRONOTE_URL",) else v  # on masque rien ici sauf si tu veux
            log(f"  - {k}={show}")

    # On ne log pas les secrets
    for secret in ("PRONOTE_USER", "PRONOTE_PASS", "GCAL_CLIENT_SECRET", "GCAL_TOKEN_JSON"):
        log(f"  - {secret}={'<set>' if os.getenv(secret) else '<absent>'}")

def maybe_print_public_ip():
    try:
        import urllib.request, json as _json
        ip = _json.loads(urllib.request.urlopen("https://api.ipify.org?format=json", timeout=5).read())["ip"]
        log(f"[NET] IP publique du job: {ip}")
    except Exception as e:
        log(f"[NET] IP publique non récupérée: {e}")

def main():
    ensure_dir(SCREEN_DIR)
    log(f"[PY] {PY}")
    log(f"[PY] {sys.version}")
    print_env_overview()
    maybe_print_public_ip()

    # 1) secrets -> fichiers
    wrote_creds = write_secret_file("credentials.json", "GCAL_CLIENT_SECRET")
    wrote_token = write_secret_file("token.json", "GCAL_TOKEN_JSON")
    if not (Path("credentials.json").exists() or wrote_creds):
        log("[WARN] credentials.json introuvable et $GCAL_CLIENT_SECRET absent. OAuth demandera un consentement local si requis.")
    if not Path("token.json").exists():
        log("[INFO] token.json non présent (normale si première exécution)")

    # 2) pip & deps
    ensure_pip()
    try:
        install_requirements()
    except Exception:
        log("[FATAL] Installation deps échouée")
        sys.exit(1)

    # 3) navigateurs Playwright
    try:
        ensure_playwright_browsers()
    except Exception:
        log("[FATAL] Install des navigateurs Playwright échouée")
        sys.exit(1)

    # 4) Exécution de la synchro
    try:
        # import direct pour garder le même interpréteur/environnement
        import pronote_playwright_to_family_mo as sync_mod
    except Exception as e:
        log(f"[FATAL] import pronote_playwright_to_family_mo KO: {e}")
        traceback.print_exc()
        sys.exit(1)

    try:
        sync_mod.run()
    except SystemExit as e:
        # le module peut faire SystemExit(1)
        rc = int(getattr(e, "code", 1) or 1)
        log(f"[FATAL] run() a levé SystemExit({rc})")
        sys.exit(rc)
    except Exception as e:
        log(f"[FATAL] Exception dans run(): {e}")
        traceback.print_exc()
        sys.exit(1)

    log("[DONE] sync terminée sans exception")

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        log(f"[FATAL] sync.py a échoué: {ex}")
        traceback.print_exc()
        sys.exit(1)
