#Requires -Version 5.1
<#
.SYNOPSIS
    Test ceda-store compatibility: fetch ceda-store's PowerShell modules
    from GitHub and run them on top of an existing 1cijferho installation.
    Verifies that ceda-store's installer works correctly when Scoop, uv,
    and the project are already set up by 1cijferho's own installer.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Helpers ────────────────────────────────────────────────────────────────────
function Write-Step  { param([string]$Msg) Write-Host "[CEDA-STORE]  $Msg" -ForegroundColor Magenta }
function Write-Ok    { param([string]$Msg) Write-Host "[OK]          $Msg" -ForegroundColor Green }
function Write-Fail  { param([string]$Msg) Write-Host "[FAIL]        $Msg" -ForegroundColor Red; exit 1 }

$cedaStoreRepo = "cedanl/ceda-store"
$cedaStoreBranch = "main"
$baseUrl = "https://raw.githubusercontent.com/$cedaStoreRepo/$cedaStoreBranch"
$modulesDir = Join-Path $env:TEMP "ceda-store-modules"

# ── 1. Fetch ceda-store PowerShell modules ────────────────────────────────────
Write-Step "Fetching ceda-store modules from GitHub ($cedaStoreRepo@$cedaStoreBranch)"

$modules = @(
    "ceda-run.ps1",
    "scoop-install.ps1",
    "detect-project.ps1",
    "uv-install.ps1",
    "uv-config.ps1",
    "uv-sync.ps1",
    "uv-run.ps1",
    "r-install.ps1",
    "r-config.ps1",
    "r-sync.ps1",
    "r-run.ps1"
)

New-Item -ItemType Directory -Path $modulesDir -Force | Out-Null

foreach ($mod in $modules) {
    $url = "$baseUrl/scripts/windows/modules/$mod"
    $dest = Join-Path $modulesDir $mod
    Write-Host "  Downloading $mod"
    Invoke-RestMethod -Uri $url -OutFile $dest
    if (-not (Test-Path $dest)) { Write-Fail "Failed to download $mod" }
}

Write-Ok "All modules downloaded to $modulesDir"

# ── 2. Run ceda-store steps via ceda-run.ps1 ─────────────────────────────────
# ceda-run.ps1 expects: -Step <step-name> -Root <project-root>
# It sources sibling modules from its own directory.
$cedaRun = Join-Path $modulesDir "ceda-run.ps1"
$projectRoot = (Get-Location).Path

$steps = @(
    "scoop-check",
    "core-deps",
    "buckets",
    "uv-install",
    "uv-config",
    "uv-sync"
)

foreach ($step in $steps) {
    Write-Step "Running step: $step"
    & $cedaRun -Step $step -Root $projectRoot
    if ($LASTEXITCODE -ne 0) { Write-Fail "ceda-store step '$step' failed (exit code $LASTEXITCODE)" }
    Write-Ok "Step '$step' completed"
}

# ── 3. Verify everything still works after ceda-store ran ─────────────────────
Write-Step "Post-compatibility verification"

scoop --version
if ($LASTEXITCODE -ne 0) { Write-Fail "Scoop broken after ceda-store run" }

uv --version
if ($LASTEXITCODE -ne 0) { Write-Fail "uv broken after ceda-store run" }

uv run python -c "import streamlit; print('streamlit', streamlit.__version__)"
if ($LASTEXITCODE -ne 0) { Write-Fail "Streamlit import failed after ceda-store run" }

uv run python -c "import polars; print('polars', polars.__version__)"
if ($LASTEXITCODE -ne 0) { Write-Fail "Polars import failed after ceda-store run" }

Write-Ok "All post-compatibility checks passed"

Write-Host "`n[DONE]  ceda-store compatibility test completed successfully" -ForegroundColor Green
