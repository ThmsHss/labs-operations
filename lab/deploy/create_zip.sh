#!/bin/bash
# Creates the PumaOpsLab.zip for Vocareum deployment.
# The zip contains all notebooks, jobs, pipelines, and hints
# in the format expected by config.json (entry = LAB_Description).

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAB_DIR="$(dirname "$SCRIPT_DIR")"
ZIP_NAME="PumaOpsLab.zip"
ZIP_PATH="$SCRIPT_DIR/$ZIP_NAME"

echo "📦 Creating $ZIP_NAME from $LAB_DIR ..."

# Remove old zip if exists
rm -f "$ZIP_PATH"

cd "$LAB_DIR"

# Zip notebooks, jobs, pipelines, hints (all .py and .sql files)
zip -r "$ZIP_PATH" \
    notebooks/*.py \
    jobs/*.py \
    pipelines/*.sql \
    hints/*.py \
    -x "*.pyc" -x "__pycache__/*"

echo "✅ Created $ZIP_PATH"
echo "   Size: $(du -h "$ZIP_PATH" | cut -f1)"
echo "   Contents:"
unzip -l "$ZIP_PATH" | tail -n +4 | head -n -2 | awk '{print "    " $4}'
