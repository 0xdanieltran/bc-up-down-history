@echo off
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo Creating venv...
  python -m venv .venv
  .\.venv\Scripts\python.exe -m pip install -r requirements.txt
)
echo Starting dashboard at http://127.0.0.1:8787
.\.venv\Scripts\python.exe run.py serve --host 127.0.0.1 --port 8787
pause
