@echo off
setlocal enabledelayedexpansion

if not exist .env (
  echo [ERROR] Arquivo .env nao encontrado. Copie o modelo e configure as variaveis.
  exit /b 1
)

set PYTHON_EXEC=python
where py >nul 2>&1
if %errorlevel% EQU 0 (
  set PYTHON_EXEC=py
)

%PYTHON_EXEC% -m pip install --upgrade pip >nul
%PYTHON_EXEC% -m pip install -q python-dotenv requests >nul
if %errorlevel% NEQ 0 (
  echo [ERROR] Falha ao instalar dependencias.
  exit /b 1
)

for %%S in (fetch_api fetch_deliveries fetch_companies fetch_email_imap build_events) do (
  echo === Executando scripts/%%S.py ===
  %PYTHON_EXEC% scripts/%%S.py
  if errorlevel 1 (
    echo [ERROR] Script %%S falhou.
    exit /b 1
  )
)

if not exist data\events.json (
  echo {}>data\events.json
)

set size=0
for %%F in (data\events.json) do set size=%%~zF
if "%size%"=="0" (
  echo [WARN] events.json vazio. Confira:
  echo  - .env (ACESSORIAS_TOKEN e IMAP)
  echo  - scripts\config.json ("statuses": [], "days_back": 120)
)

echo Pipeline concluido com sucesso.
endlocal
