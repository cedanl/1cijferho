@echo off
:: 1CijferHO Scoop Installer
:: Double-click this file to install all dependencies.
set "psScript=%~dp0ps\scoop-install.ps1"
if not exist "%psScript%" (
    echo Error: scoop-install.ps1 not found in ps folder!
    pause
    exit /b 1
)
powershell -ExecutionPolicy Bypass -File "%psScript%"
pause
