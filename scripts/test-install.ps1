#Requires -Version 5.1
<#
.SYNOPSIS
    Install Scoop, uv, and project dependencies for 1CijferHO.
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
# OneDrive / VHD mounts can sit on a different volume than TEMP,
# causing uv's default hardlink strategy to fail.
if (-not $env:UV_LINK_MODE) {
    $env:UV_LINK_MODE = "copy"
    Write-Host "[INFO]  UV_LINK_MODE set to 'copy' (filesystem boundary safety)" -ForegroundColor Yellow
}

# ── 1. Install Scoop ──────────────────────────────────────────────────────────
Write-Step "Installing Scoop"

# Set execution policy (ignore errors — may already be Bypass from the caller)
try { Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force } catch {}

# If SCOOP env var isn't set, default to the per-user location
if (-not $env:SCOOP) {
    $env:SCOOP = Join-Path $env:USERPROFILE "scoop"
}

Invoke-RestMethod -Uri https://get.scoop.sh | Invoke-Expression

# Make sure shims are on PATH for this session and future steps
$scoopShims = Join-Path $env:SCOOP "shims"
if ($env:PATH -notlike "*$scoopShims*") {
    $env:PATH = "$scoopShims;$env:PATH"
}
# Persist for subsequent GitHub Actions steps
if ($env:GITHUB_PATH) {
    Add-Content -Path $env:GITHUB_PATH -Value $scoopShims
}

scoop --version
if ($LASTEXITCODE -ne 0) { Write-Fail "Scoop installation failed" }
Write-Ok "Scoop installed"

# ── 2. Install uv via Scoop ───────────────────────────────────────────────────
Write-Step "Installing uv"

scoop install uv
if ($LASTEXITCODE -ne 0) { Write-Fail "uv installation failed" }

uv --version
if ($LASTEXITCODE -ne 0) { Write-Fail "uv not on PATH after install" }
Write-Ok "uv installed: $(uv --version)"

# ── 3. Sync project dependencies ──────────────────────────────────────────────
Write-Step "Syncing project dependencies (uv sync)"

uv sync
if ($LASTEXITCODE -ne 0) { Write-Fail "uv sync failed" }
Write-Ok "Dependencies synced"

# ── 4. Quick smoke test ───────────────────────────────────────────────────────
Write-Step "Smoke-testing Python + Streamlit import"

uv run python -c "import streamlit; print('streamlit', streamlit.__version__)"
if ($LASTEXITCODE -ne 0) { Write-Fail "Streamlit import failed" }
Write-Ok "Smoke test passed"

Write-Host "`n[DONE]  Installation completed successfully" -ForegroundColor Green
