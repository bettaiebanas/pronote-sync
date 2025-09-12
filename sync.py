# sync.py — orchestrateur pour pronote_playwright_to_family_mo.py
# ASCII-only logs to avoid UnicodeEncodeError on Windows consoles.

import os
import sys
import json
import subprocess
import shutil
import textwrap
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY   = sys.executable  # Python used by setup-python on the runner

def log(msg):
    """Print in a console-safe way (ASCII only)."""
    try:
        # keep it simple and safe for cp1252 consoles
        print(str(msg).encode("ascii", "replace").decode("ascii"), flush=True)
    except Exception:
        # last resort
        print(str(msg), flush=True)

def run(cmd, check=True):
    """Run a subprocess, printing the command."""
    show = " ".join(cmd) if isinstance(cmd, list) else cmd
    log("[RUN] " + show)
    return subprocess.run(cmd, check=check)

def write_if_env(env_name, dest_path):
    """Write dest_path if the env var is present."""
    val = os.getenv(env_name)
    if not val:
        log(f"[CFG] {env_name}: (absent) - not writing {dest_path}")
        return False
    Path(dest_path).write_text(val, encoding="utf-8")
    log(f"[CFG] {env_name}: written -> {dest_path}")
    return True

def ensure_pip():
    """Ensure pip is present and up to date."""
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
    """Install libs required by the Pronote script."""
    pkgs = [
        "playwright",
        "google-api-python-client",
        "google-auth-httplib2",
        "google-auth-oauthlib",
        "python-dateutil",
    ]
    run([PY, "-m", "pip", "install", "-q"] + pkgs, check=True)

def playwright_install_browsers():
    """Install Chromium for Playwright."""
    run([PY, "-m", "playwright", "install", "chromium"], check=True)

def ascii_sample(s, maxlen=80):
    """Return a short ASCII-safe sample of a string."""
    if s is None:
        return ""
    try:
        s = str(s)
        if len(s) > maxlen:
            s = s[:maxlen] + "..."
        return s.encode("ascii", "replace").decode("ascii")
    except Exception:
        return "<non-printable>"

def show_env_debug():
    keys = [
        "PRONOTE_USER", "PRONOTE_PASS",
        "ENT_URL", "PRONOTE_URL",
        "TIMETABLE_PRE_SELECTOR", "TIMETABLE_SELECTOR", "TIMETABLE_FRAME",
        "WEEK_TAB_TEMPLATE", "FETCH_WEEKS_FROM", "WEEKS_TO_FETCH",
        "HEADFUL", "CALENDAR_ID",
        "GCAL_CLIENT_SECRET", "GCAL_TOKEN_JSON",
    ]
    log("[DBG] Env keys (presence only):")
    for k in keys:
        present = "YES" if os.getenv(k) else "NO"
        if k in ("PRONOTE_PASS", "GCAL_CLIENT_SECRET", "GCAL_TOKEN_JSON"):
            log(f"   - {k}: {present}")
        else:
            v = ascii_sample(os.getenv(k, ""))
            log(f"   - {k}: {present} '{v}'")

def main():
    log("=== sync.py: preparing execution ===")

    # 1) OAuth credentials (from secrets)
    wrote_cred = write_if_env("GCAL_CLIENT_SECRET", ROOT / "credentials.json")
    wrote_tok  = write_if_env("GCAL_TOKEN_JSON",   ROOT / "token.json")
    if not wrote_cred:
        log("[INFO] GCAL_CLIENT_SECRET not provided - if token.json is valid, that is fine.")
    if not wrote_tok:
        log("[INFO] GCAL_TOKEN_JSON not provided - script may trigger local OAuth flow if needed.")

    # 2) Pip + deps
    ensure_pip()
    pip_install()

    # 3) Playwright browsers
    playwright_install_browsers()

    # 4) Debug env (ASCII)
    show_env_debug()

    # 5) Launch main script
    target = ROOT / "pronote_playwright_to_family_mo.py"
    if not target.exists():
        log(f"[FATAL] Missing file: {target}")
        sys.exit(2)

    log("=== Launching pronote_playwright_to_family_mo.py ===")
    try:
        run([PY, str(target)], check=True)
        log("=== Script finished without error ===")
    except subprocess.CalledProcessError as cpe:
        log(f"[FATAL] Pronote script returned non-zero code ({cpe.returncode}).")
        sys.exit(cpe.returncode)

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        log("[FATAL] Unhandled exception in sync.py:")
        traceback.print_exc()
        sys.exit(1)
