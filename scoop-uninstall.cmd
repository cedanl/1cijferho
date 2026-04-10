@echo off
:: 1CijferHO Scoop Uninstaller
:: Double-click this file to remove Scoop and all installed dependencies.
set "psScript=%~dp0ps\scoop-uninstall.ps1"
if not exist "%psScript%" (
    echo Error: scoop-uninstall.ps1 not found in ps folder!
    pause
    exit /b 1
)
powershell -ExecutionPolicy Bypass -File "%psScript%"
pause
