# sync.py — orchestrateur pour pronote_playwright_to_family_mo.py

import os
import sys
import json
import subprocess
import shutil
import textwrap
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY   = sys.executable  # Python courant (celui de setup-python sur le runner)

def log(msg):
    print(msg, flush=True)

def run(cmd, check=True):
    """Exécute un processus enfant en affichant la commande."""
    if isinstance(cmd, list):
        show = " ".join(cmd)
    else:
        show = cmd
    log(f"[RUN] {show}")
    return subprocess.run(cmd, check=check)

def write_if_env(env_name, dest_path):
    """Écrit le fichier dest_path si la variable d'env est définie."""
    val = os.getenv(env_name)
    if not val:
        log(f"[CFG] {env_name}: (absent) – je n'écris pas {dest_path}")
        return False
    Path(dest_path).write_text(val, encoding="utf-8")
    log(f"[CFG] {env_name}: écrit -> {dest_path}")
    return True

def ensure_pip():
    """S’assure que pip est disponible et à jour."""
    try:
        run([PY, "-m", "ensurepip", "--upgrade"], check=False)
    except Exception as e:
        log(f"[WARN] ensurepip: {e}")
    try:
        run([PY, "-m", "pip", "--version"], check=False)
        run([PY, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"], check=False)
    except Exception as e:
        log(f"[WARN] upgrade pip: {e}")

def pip_install():
    """Installe les libs nécessaires au script Pronote."""
    pkgs = [
        "playwright",
        "google-api-python-client",
        "google-auth-httplib2",
        "google-auth-oauthlib",
        "python-dateutil",
    ]
    run([PY, "-m", "pip", "install", "-q"] + pkgs, check=True)

def playwright_install_browsers():
    """Installe Chromium pour Playwright (nécessaire côté runner Windows)."""
    run([PY, "-m", "playwright", "install", "chromium"], check=True)

def show_env_debug():
    keys = [
        "PRONOTE_USER", "PRONOTE_PASS",
        "ENT_URL", "PRONOTE_URL",
        "TIMETABLE_PRE_SELECTOR", "TIMETABLE_SELECTOR", "TIMETABLE_FRAME",
        "WEEK_TAB_TEMPLATE", "FETCH_WEEKS_FROM", "WEEKS_TO_FETCH",
        "HEADFUL", "CALENDAR_ID",
        "GCAL_CLIENT_SECRET", "GCAL_TOKEN_JSON",
    ]
    log("[DBG] Environnement (présence des clés seulement) :")
    for k in keys:
        present = "✅" if os.getenv(k) else "—"
        if k in ("PRONOTE_PASS", "GCAL_CLIENT_SECRET", "GCAL_TOKEN_JSON"):
            log(f"   - {k}: {present}")
        else:
            v = os.getenv(k, "")
            if len(v) > 80:
                v = v[:80] + "…"
            log(f"   - {k}: {present} {v!r}")

def main():
    log("=== sync.py : préparation de l’exécution ===")

    # 1) Credentials OAuth Google (depuis secrets)
    wrote_cred = write_if_env("GCAL_CLIENT_SECRET", ROOT / "credentials.json")
    wrote_tok  = write_if_env("GCAL_TOKEN_JSON",   ROOT / "token.json")
    if not wrote_cred:
        log("[INFO] Pas de GCAL_CLIENT_SECRET dans l’env – si le token.json est déjà valide, ça passe quand même.")
    if not wrote_tok:
        log("[INFO] Pas de GCAL_TOKEN_JSON dans l’env – le script fera le flux OAuth local si besoin (HEADFUL conseillé).")

    # 2) Pip + dépendances
    ensure_pip()
    pip_install()

    # 3) Navigateurs Playwright
    playwright_install_browsers()

    # 4) Un peu de debug côté env
    show_env_debug()

    # 5) Lancement du script principal
    target = ROOT / "pronote_playwright_to_family_mo.py"
    if not target.exists():
        log(f"[FATAL] Fichier introuvable: {target}")
        sys.exit(2)

    log("=== Lancement de pronote_playwright_to_family_mo.py ===")
    try:
        run([PY, str(target)], check=True)
        log("=== Exécution terminée sans erreur côté script Pronote ===")
    except subprocess.CalledProcessError as cpe:
        log(f"[FATAL] Le script Pronote a renvoyé un code ≠ 0 ({cpe.returncode}).")
        sys.exit(cpe.returncode)

if __name__ == "__main__":
    try:
        main()
    except SystemExit as se:
        raise
    except Exception:
        log("[FATAL] Exception non gérée dans sync.py :")
        traceback.print_exc()
        sys.exit(1)
