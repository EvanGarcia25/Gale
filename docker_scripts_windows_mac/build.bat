@echo off
REM Build the Docker image
REM Usage: build.bat [tag]

setlocal enabledelayedexpansion

if "%1"=="" (
    set TAG=yearbook-scraper:latest
) else (
    set TAG=%1
)

echo Building Docker image: !TAG!
docker build -t !TAG! .

if !errorlevel! equ 0 (
    echo.
    echo [+] Build successful!
    echo.
    echo To run the scraper:
    echo   docker run -v C:\path\to\output:/app/yearbook_downloads -v C:\path\to\logs:/app/scraper_logs !TAG!
) else (
    echo [-] Build failed
    exit /b 1
)
