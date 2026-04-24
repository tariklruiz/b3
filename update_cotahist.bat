@echo off
rem ============================================================================
rem FII Guia - Daily COTAHIST updater
rem ----------------------------------------------------------------------------
rem Usage:
rem   Double-click          -> finds newest COTAHIST_*.TXT in Downloads folder
rem   Drag-and-drop a file  -> processes that specific file
rem   Command line          -> update_cotahist.bat "C:\path\to\COTAHIST_D260421.TXT"
rem
rem Prerequisites:
rem   - Python with polars, psycopg2-binary, python-dotenv installed
rem   - .env file in this folder with DATABASE_URL set
rem ============================================================================

setlocal

rem --- Jump to the directory where this .bat lives ----------------------------
cd /d "%~dp0"

rem --- Pick the file to process -----------------------------------------------
if "%~1"=="" (
    rem No argument: find the newest COTAHIST_*.TXT in Downloads
    echo No file specified - searching for newest COTAHIST_*.TXT in Downloads...
    set "COTAHIST_FILE="
    for /f "delims=" %%f in ('dir /b /o-d "%USERPROFILE%\Downloads\COTAHIST_*.TXT" 2^>nul') do (
        if not defined COTAHIST_FILE set "COTAHIST_FILE=%USERPROFILE%\Downloads\%%f"
    )
    if not defined COTAHIST_FILE (
        echo.
        echo ERROR: No COTAHIST_*.TXT file found in %USERPROFILE%\Downloads
        echo.
        echo You can either:
        echo   1. Download today's COTAHIST file from B3 to your Downloads folder
        echo   2. Drag a COTAHIST file onto this .bat
        echo.
        pause
        exit /b 1
    )
    echo Found: %COTAHIST_FILE%
) else (
    set "COTAHIST_FILE=%~1"
)

rem --- Verify the file exists --------------------------------------------------
if not exist "%COTAHIST_FILE%" (
    echo.
    echo ERROR: File not found: %COTAHIST_FILE%
    echo.
    pause
    exit /b 1
)

rem --- Run the parser ----------------------------------------------------------
echo.
echo ============================================================
echo  Processing: %COTAHIST_FILE%
echo ============================================================
echo.

"C:\Users\tarik.lauar\AppData\Local\anaconda3\python.exe" parser.py --file "%COTAHIST_FILE%"
set "RC=%ERRORLEVEL%"

echo.
if %RC% NEQ 0 (
    echo ============================================================
    echo  FAILED  ^(exit code %RC%^)
    echo ============================================================
) else (
    echo ============================================================
    echo  SUCCESS - Postgres is up to date
    echo ============================================================
)

echo.
pause
exit /b %RC%
