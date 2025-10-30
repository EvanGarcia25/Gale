@echo off
REM Run the Docker container
REM Usage: run.bat [output_path] [logs_path] [image_tag]

setlocal enabledelayedexpansion

if "%1"=="" (
    set OUTPUT_PATH=%cd%\.yearbook_downloads
) else (
    set OUTPUT_PATH=%1
)

if "%2"=="" (
    set LOGS_PATH=%cd%\.scraper_logs
) else (
    set LOGS_PATH=%2
)

if "%3"=="" (
    set IMAGE=yearbook-scraper:latest
) else (
    set IMAGE=%3
)

REM Create directories if they don't exist
if not exist "%OUTPUT_PATH%" mkdir "%OUTPUT_PATH%"
if not exist "%LOGS_PATH%" mkdir "%LOGS_PATH%"

echo Running Docker container...
echo Output folder: %OUTPUT_PATH%
echo Logs folder: %LOGS_PATH%
echo.

docker run ^
    --name yearbook-scraper-run ^
    -v "%OUTPUT_PATH%:/app/yearbook_downloads" ^
    -v "%LOGS_PATH%:/app/scraper_logs" ^
    --rm ^
    %IMAGE%

if !errorlevel! equ 0 (
    echo.
    echo [+] Scraper completed successfully!
    echo Output at: %OUTPUT_PATH%
    echo Logs at: %LOGS_PATH%
) else (
    echo.
    echo [-] Scraper failed. Check logs at: %LOGS_PATH%
    exit /b 1
)
