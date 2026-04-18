@echo off
setlocal enabledelayedexpansion
echo ================================================
echo  B3 Database Update
echo ================================================
echo.

set PYTHON=C:\Users\tarik.lauar\AppData\Local\anaconda3\python.exe
set ROOT=C:\Users\tarik.lauar\Dropbox\Personal Tarik\01 - Documentos\b3app
set DATA=%ROOT%\backend\data

rem ----------------------------------------------------------------
set RAILWAY_TOKEN=5d55fbcb-12c4-43d2-aba4-33ae14702145
set RAILWAY_SERVICE_ID=adbd96c0-e30a-45e1-931e-55dee40041c3
set RAILWAY_ENV_ID=054115c8-ec8a-493f-9b6e-8ad6d5ee3d69
rem ----------------------------------------------------------------

rem ── Step 1: Process any COTAHIST files ───────────────────────────
echo [1/5] Verificando COTAHIST...

rem Use dir to check if any COTAHIST file exists first
dir /b "%DATA%\COTAHIST_A????.TXT" >nul 2>&1
if errorlevel 1 (
    echo   Nenhum arquivo COTAHIST encontrado — pulando parse.
    echo.
    goto step2
)

rem Process each file found
for %%A in ("%DATA%\COTAHIST_A????.TXT") do (
    set "FULL_PATH=%%~fA"
    set "BASE_NAME=%%~nA"
    set "FULL_NAME=%%~nxA"

    rem Extract year — name is COTAHIST_AYYYY, year is last 4 chars
    set "YEAR=!BASE_NAME:~-4!"

    echo   Arquivo: !FULL_NAME!
    echo   Ano extraido: !YEAR!

    rem Safety check — year must be 4 digits
    echo !YEAR!| findstr /r "^[0-9][0-9][0-9][0-9]$" >nul
    if errorlevel 1 (
        echo   ERRO: ano invalido '!YEAR!' — abortando para este arquivo.
        goto step2
    )

    echo.
    echo   Removendo dados de !YEAR! do banco...
    "%PYTHON%" "%ROOT%\delete_year.py" !YEAR!
    echo.

    echo   Carregando !FULL_NAME!...
    "%PYTHON%" "%ROOT%\parser.py" --file "!FULL_PATH!" --db "%DATA%\b3.db" --append
    echo.

    echo   Deletando !FULL_NAME!...
    del "!FULL_PATH!"
    if errorlevel 1 (
        echo   AVISO: nao foi possivel deletar !FULL_NAME!
    ) else (
        echo   Arquivo deletado.
    )
    echo.
)

:step2
rem ── Step 2: Copy fund_types.json to data folder ──────────────────
echo [2/5] Copiando fund_types.json para data/...
if exist "%ROOT%\fund_types.json" (
    copy /Y "%ROOT%\fund_types.json" "%DATA%\fund_types.json" >nul
    echo   OK — fund_types.json copiado.
) else (
    echo   AVISO: fund_types.json nao encontrado — execute scraper_classificacao.py primeiro.
)
echo.

rem ── Step 3: VACUUM all databases ─────────────────────────────────
echo [3/5] VACUUM em todos os bancos...
"%PYTHON%" "%ROOT%\vacuum_db.py"
echo.

rem ── Step 4: Wait for Dropbox to sync ─────────────────────────────
echo [4/5] Aguardando Dropbox sincronizar (60s)...
timeout /t 60 /nobreak >nul
echo   Pronto.
echo.

rem ── Step 5: Trigger Railway redeploy ─────────────────────────────
echo [5/5] Triggering Railway redeploy...
curl -s -X POST "https://backboard.railway.app/graphql/v2" ^
  -H "Authorization: Bearer %RAILWAY_TOKEN%" ^
  -H "Content-Type: application/json" ^
  -d "{\"query\": \"mutation { serviceInstanceRedeploy(serviceId: \\\"%RAILWAY_SERVICE_ID%\\\", environmentId: \\\"%RAILWAY_ENV_ID%\\\") }\"}"
echo.
echo Railway redeploy triggered.
echo.

echo ================================================
echo  Pronto! Railway vai reconstruir com os dados atualizados.
echo  Acompanhe em: https://railway.app/project/63d9c648-d51a-40f3-92ea-339b0271fa82
echo ================================================
echo.
pause
