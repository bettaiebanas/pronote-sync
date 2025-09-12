# -*- coding: utf-8 -*-
"""
Petit wrapper utilitaire : journalise l'environnement, prépare les fichiers OAuth
si fournis, puis lance le script PRONOTE. Le workflow ci-dessus peut appeler directement
pronote_playwright_to_family_mo.py ; ce wrapper est facultatif.
"""
import os, subprocess, sys

def log(msg): print(msg, flush=True)

def main():
    log("=== sync.py: preparing execution ===")
    # Fichiers OAuth si Secrets présents
    c = os.getenv("GCAL_CLIENT_SECRET", "")
    t = os.getenv("GCAL_TOKEN_JSON", "")
    if c:
        open("credentials.json","w", encoding="utf-8").write(c)
        log("[CFG] credentials.json écrit (via secret).")
    else:
        log("[CFG] GCAL_CLIENT_SECRET absent - pas d'écriture de credentials.json")

    if t:
        open("token.json","w", encoding="utf-8").write(t)
        log("[CFG] token.json écrit (via secret).")
    else:
        log("[CFG] GCAL_TOKEN_JSON absent - pas d'écriture de token.json")

    # Affiche les clés (présence uniquement)
    keys = ["PRONOTE_USER","PRONOTE_PASS","ENT_URL","PRONOTE_URL","TIMETABLE_PRE_SELECTOR",
            "TIMETABLE_SELECTOR","TIMETABLE_FRAME","WEEK_TAB_TEMPLATE","FETCH_WEEKS_FROM",
            "WEEKS_TO_FETCH","HEADFUL","CALENDAR_ID"]
    log("[DBG] Env keys (presence only):")
    for k in keys:
        v = os.getenv(k)
        log(f"  - {k}: {'YES' if v else 'NO'}")

    # Lancer le script principal
    py = sys.executable
    cmd = [py, os.path.abspath("pronote_playwright_to_family_mo.py")]
    log("=== Launching pronote_playwright_to_family_mo.py ===")
    rc = subprocess.call(cmd)
    if rc != 0:
        log(f"[FATAL] pronote script exited with code {rc}")
        sys.exit(rc)

if __name__ == "__main__":
    main()
