#!/usr/bin/env bash
set -e

NOTEBOOK="20250115 이상 상태 탐지 모델 구동 코드.ipynb"
SCRIPT="${NOTEBOOK%.ipynb}.py"

# 1. Convert the notebook to a Python script
jupyter nbconvert --to script "$NOTEBOOK" --output-dir /app

# 2. Execute the generated script
python "/app/$SCRIPT"
