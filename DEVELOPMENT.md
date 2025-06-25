# Development Guide

This document explains how to set up the superclient library for local development.

## Local Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/superstreamlabs/superclient-python.git
cd superclient-python
```

### Step 2: Create a Virtual Environment (Recommended)

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install in Development Mode

```bash
pip install -e . && python -m superclient install_pth
```

This installs the package in "editable" mode and enables automatic loading, which means:
- Changes to the source code are immediately reflected without reinstalling
- The package is installed in your Python environment
- You can import and use the package normally
- The `.pth` file is installed to enable automatic loading when Python starts

## Development Workflow

### Making Changes

1. Edit the source code in the `superclient/` directory
2. Changes are immediately available (no reinstallation needed)
3. Test your changes by running examples or your own code


## Uninstallation

```bash
pip uninstall superclient && find venv/lib/python*/site-packages -name "superclient-init.pth" -delete && rm -rf build/ dist/ superclient.egg-info/ && find . -name "*.pyc" -delete && find . -name "__pycache__" -type d -exec rm -rf {} +
```

This single command:
1. Uninstalls the superclient package
2. Removes the .pth file
3. Cleans up build artifacts
4. Removes cached Python files