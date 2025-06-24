"""Allow running superclient module utilities"""
import sys

if len(sys.argv) > 1 and sys.argv[1] == "install_pth":
    from .install_pth import install_pth
    install_pth()
else:
    print("Usage: python -m superclient install_pth") 