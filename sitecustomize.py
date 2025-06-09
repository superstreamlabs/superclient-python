try:
    import superclient
except Exception as e:
    import sys
    sys.stderr.write(f"[superstream] ERROR [ERR-000] Failed to import superclient: {str(e)}\n")
    sys.stderr.flush() 