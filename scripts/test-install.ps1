#Requires -Version 5.1
<#
.SYNOPSIS
    CI wrapper: runs the main scoop-install.ps1 in non-interactive mode.
    Designed to work for both admin and standard users,
    and on filesystems with OneDrive known-folder redirection.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Helpers ────────────────────────────────────────────────────────────────────
function Write-Step  { param([string]$Msg) Write-Host "[STEP]  $Msg" -ForegroundColor Cyan }
function Write-Ok    { param([string]$Msg) Write-Host "[OK]    $Msg" -ForegroundColor Green }
function Write-Fail  { param([string]$Msg) Write-Host "[FAIL]  $Msg" -ForegroundColor Red; exit 1 }

# ── Guard against hardlink issues across filesystem boundaries ─────────────────
if (-not $env:UV_LINK_MODE) {
    $env:UV_LINK_MODE = "copy"
    Write-Host "[INFO]  UV_LINK_MODE set to 'copy' (filesystem boundary safety)" -ForegroundColor Yellow
}

# ── Run the main installer ────────────────────────────────────────────────────
Write-Step "Running scoop-install.ps1 -NonInteractive"

$installer = Join-Path $PSScriptRoot "scoop-install.ps1"
& $installer -NonInteractive
if ($LASTEXITCODE -ne 0) { Write-Fail "scoop-install.ps1 failed" }

Write-Ok "Main installer completed"

Write-Host "`n[DONE]  Installation completed successfully" -ForegroundColor Green
