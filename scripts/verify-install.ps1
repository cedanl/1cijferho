#Requires -Version 5.1
<#
.SYNOPSIS
    Verify that the installation completed correctly.
    Checks that Scoop, uv, Python, and Streamlit are functional
    and that no files leaked into unexpected locations.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$failed = 0

function Write-Check { param([string]$Msg) Write-Host "[CHECK] $Msg" -ForegroundColor Cyan }
function Write-Ok    { param([string]$Msg) Write-Host "[OK]    $Msg" -ForegroundColor Green }
function Write-Fail  {
    param([string]$Msg)
    Write-Host "[FAIL]  $Msg" -ForegroundColor Red
    $script:failed++
}

# ── 1. Scoop on PATH and responding ───────────────────────────────────────────
Write-Check "Scoop is available"
$scoopCmd = Get-Command scoop -ErrorAction SilentlyContinue
if ($scoopCmd) {
    $scoopVer = scoop --version 2>&1
    Write-Ok "scoop found at $($scoopCmd.Source) — $scoopVer"
} else {
    Write-Fail "scoop not found on PATH"
}

# ── 2. uv on PATH and responding ─────────────────────────────────────────────
Write-Check "uv is available"
$uvCmd = Get-Command uv -ErrorAction SilentlyContinue
if ($uvCmd) {
    $uvVer = uv --version 2>&1
    Write-Ok "uv found at $($uvCmd.Source) — $uvVer"
} else {
    Write-Fail "uv not found on PATH"
}

# ── 3. Virtual environment exists ─────────────────────────────────────────────
Write-Check ".venv directory exists"
if (Test-Path ".venv") {
    Write-Ok ".venv present"
} else {
    Write-Fail ".venv directory not found (uv sync may not have run)"
}

# ── 4. Python version matches requirement ─────────────────────────────────────
Write-Check "Python 3.13.x in venv"
$pyVer = uv run python --version 2>&1
if ($LASTEXITCODE -eq 0 -and $pyVer -match "3\.13") {
    Write-Ok "$pyVer"
} else {
    Write-Fail "Expected Python 3.13.x, got: $pyVer"
}

# ── 5. Streamlit importable ───────────────────────────────────────────────────
Write-Check "Streamlit imports successfully"
$slVer = uv run python -c "import streamlit; print(streamlit.__version__)" 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Ok "streamlit $slVer"
} else {
    Write-Fail "Streamlit import failed: $slVer"
}

# ── 6. Polars importable ─────────────────────────────────────────────────────
Write-Check "Polars imports successfully"
$plVer = uv run python -c "import polars; print(polars.__version__)" 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Ok "polars $plVer"
} else {
    Write-Fail "Polars import failed: $plVer"
}

# ── 7. main.py compiles without errors ────────────────────────────────────────
Write-Check "src/main.py compiles"
uv run python -m py_compile src/main.py 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Ok "src/main.py syntax valid"
} else {
    Write-Fail "src/main.py has syntax errors"
}

# ── 8. No files in unexpected locations (OneDrive sim check) ──────────────────
Write-Check "No Scoop artifacts in OneDrive-redirected folders"

# Patterns that are expected — PowerShell and Windows itself write to Documents
$ignoredPatterns = @(
    "*\PowerShell",
    "*\PowerShell\*",
    "*\WindowsPowerShell",
    "*\WindowsPowerShell\*",
    "*.ps1xml"
)

$unexpected = @()
$folders = @($env:ONEDRIVE_SIM_DOCUMENTS, $env:ONEDRIVE_SIM_DOWNLOADS)
foreach ($folder in $folders) {
    if ($folder -and (Test-Path $folder)) {
        $items = Get-ChildItem -Path $folder -Recurse -ErrorAction SilentlyContinue
        foreach ($item in $items) {
            $dominated = $false
            foreach ($pat in $ignoredPatterns) {
                if ($item.FullName -like $pat) { $dominated = $true; break }
            }
            if (-not $dominated) { $unexpected += $item }
        }
    }
}

if ($unexpected.Count -gt 0) {
    Write-Fail "Found $($unexpected.Count) unexpected file(s) in OneDrive-redirected folders:"
    $unexpected | ForEach-Object { Write-Host "        $($_.FullName)" -ForegroundColor Yellow }
} else {
    Write-Ok "No artifacts leaked into redirected known folders"
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
if ($failed -gt 0) {
    Write-Host "[RESULT] $failed check(s) FAILED" -ForegroundColor Red
    exit 1
} else {
    Write-Host "[RESULT] All checks passed" -ForegroundColor Green
    exit 0
}
