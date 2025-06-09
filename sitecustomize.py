try:
    import superclient
except Exception as e:
    import sys
    sys.stderr.write(f"[superstream] ERROR [ERR-001] Failed to import superclient: {str(e)}\n")
    sys.stderr.flush() 