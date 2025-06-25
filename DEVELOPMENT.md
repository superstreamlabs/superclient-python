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
pip install -e .
```

This installs the package in "editable" mode, which means:
- Changes to the source code are immediately reflected without reinstalling
- The package is installed in your Python environment
- You can import and use the package normally

### Step 4: Enable Automatic Loading

To test the automatic loading functionality:

```bash
python -m superclient install_pth
```

This installs the `.pth` file that enables automatic loading when Python starts.

## Development Workflow

### Making Changes

1. Edit the source code in the `superclient/` directory
2. Changes are immediately available (no reinstallation needed)
3. Test your changes by running examples or your own code


## Uninstallation

### Step 1: Remove the .pth File (if installed)

```bash
# Find and remove the .pth file
find venv/lib/python*/site-packages -name "superclient-init.pth" -delete
```

### Step 2: Uninstall the Package

```bash
pip uninstall superclient
```

### Step 3: Clean Up Build Artifacts

```bash
# Remove build directories
rm -rf build/ dist/ superclient.egg-info/

# Remove any cached Python files
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} +
```