#!/usr/bin/env python3
"""Install the superclient-init.pth file to enable automatic loading"""
import os
import sys
import shutil
import site

def install_pth():
    """Install the .pth file to the appropriate site-packages directory"""
    # Get the path to the .pth file from package resources
    import superclient
    package_dir = os.path.dirname(superclient.__file__)
    source = os.path.join(package_dir, "superclient-init.pth")
    
    if not os.path.exists(source):
        print(f"Error: {source} not found")
        return False
    
    # Find site-packages directory
    site_packages = None
    for path in sys.path:
        if path.endswith('site-packages') and os.path.isdir(path):
            site_packages = path
            break
    
    if not site_packages:
        site_packages = site.getsitepackages()[0]
    
    dest = os.path.join(site_packages, "superclient-init.pth")
    
    try:
        shutil.copy2(source, dest)
        print(f"Successfully installed {dest}")
        print("Superclient will now load automatically!")
        return True
    except Exception as e:
        print(f"Error installing .pth file: {e}")
        print(f"You may need to run this script with sudo or as administrator")
        return False

if __name__ == "__main__":
    install_pth() 