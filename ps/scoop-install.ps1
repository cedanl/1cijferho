param(
    [switch]$NonInteractive
)

$ErrorActionPreference = "Continue"

# ============================================
#  FUNCTIONS
# ============================================
function Show-Progress {
    param([int]$DurationMs = 1000)
    if ($script:NonInteractive) { return }
    $barLength = 30
    $steps = 20
    $delay = $DurationMs / $steps
    Write-Host ""
    Write-Host "[" -NoNewline -ForegroundColor DarkGray
    for ($i = 0; $i -lt $barLength; $i++) {
        Write-Host "=" -NoNewline -ForegroundColor Yellow
        Start-Sleep -Milliseconds $delay
    }
    Write-Host "]" -ForegroundColor DarkGray
    Write-Host ""
}

function Show-CompletionPrompt {
    if ($script:NonInteractive) { return }
    Write-Host ""
    Write-Host ""
    Write-Host " =============================" -ForegroundColor Green
    Write-Host " >> ALLE STAPPEN VOLTOOID <<" -ForegroundColor Green
    Write-Host " =============================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Wil je de tool opnieuw uitvoeren? " -NoNewline -ForegroundColor Yellow
    Write-Host -NoNewline "(J/n) " -ForegroundColor Cyan
    $response = Read-Host
    if ($response -eq "j") {
        Write-Host "`nTool opnieuw starten..." -ForegroundColor Cyan
        Start-Sleep -Milliseconds 500
        & $PSCommandPath
    } else {
        Write-Host "`nAfsluiten. Tot ziens!" -ForegroundColor Green
    }
}

# ============================================
#  [0] MENU
# ============================================
if (-not $NonInteractive) {
    $options = @("Setup Starten", "Afsluiten")
    $selected = 0

    do {
        Clear-Host
        Write-Host ""
        Write-Host " ===================================" -ForegroundColor Cyan
        Write-Host " 1CIJFERHO SETUP" -ForegroundColor Cyan
        Write-Host " ===================================" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "LET OP: Dit hulpmiddel heeft geen invloed op geinstalleerde applicaties en vereist geen beheerdersrechten." -ForegroundColor Yellow
        Write-Host ""
        Write-Host " Dit script zal:" -ForegroundColor Gray
        Write-Host "  * Scoop & buckets installeren" -ForegroundColor Gray
        Write-Host "  * 7zip, aria2 & git installeren voor geoptimaliseerde installatie" -ForegroundColor Gray
        Write-Host "  * uv (Python package manager) installeren" -ForegroundColor Gray
        Write-Host "  * Projectafhankelijkheden synchroniseren" -ForegroundColor Gray
        Write-Host "  * Installatie verifieren" -ForegroundColor Gray
        Write-Host ""
        Write-Host " Developed by: " -NoNewline -ForegroundColor DarkGray
        Write-Host " CEDA " -ForegroundColor Cyan
        Write-Host ""
        Write-Host " ===============================================" -ForegroundColor DarkGray
        Write-Host ""

        for ($i = 0; $i -lt $options.Count; $i++) {
            if ($i -eq $selected) {
                Write-Host " > " -NoNewline -ForegroundColor Green
                Write-Host $options[$i] -ForegroundColor Green
            } else {
                Write-Host "   $($options[$i])" -ForegroundColor Gray
            }
        }

        Write-Host ""
        Write-Host " Gebruik de pijltjestoetsen om te selecteren, druk op ENTER om te bevestigen" -ForegroundColor DarkGray

        $key = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
        if ($key.VirtualKeyCode -eq 38) { $selected = ($selected - 1 + $options.Count) % $options.Count }
        elseif ($key.VirtualKeyCode -eq 40) { $selected = ($selected + 1) % $options.Count }
    } while ($key.VirtualKeyCode -ne 13)

    Write-Host ""
    if ($selected -eq 1) {
        Write-Host " Afsluiten..." -ForegroundColor Yellow
        exit 0
    }
}

# ============================================
#  [1] SCOOP & BUCKETS
# ============================================
Write-Host "[1] Scoop en buckets installeren..." -ForegroundColor Yellow

# Set execution policy (ignore errors — may already be Bypass)
try { Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force } catch {}

if (-not (Get-Command scoop -ErrorAction SilentlyContinue)) {
    Invoke-RestMethod -Uri 'https://get.scoop.sh' | Invoke-Expression
} else {
    Write-Host "    Scoop is al geinstalleerd, stap overgeslagen..." -ForegroundColor Gray
}

# Ensure shims are on PATH for this session
$scoopDir = $env:SCOOP
if (-not $scoopDir) { $scoopDir = Join-Path $env:USERPROFILE "scoop" }
$scoopShims = Join-Path $scoopDir "shims"
if ($env:PATH -notlike "*$scoopShims*") {
    $env:PATH = "$scoopShims;$env:PATH"
}
# Persist for subsequent GitHub Actions steps
if ($env:GITHUB_PATH) {
    Add-Content -Path $env:GITHUB_PATH -Value $scoopShims
}

scoop bucket add extras
scoop bucket add versions

scoop --version
if ($LASTEXITCODE -ne 0) { Write-Host "    Scoop installatie mislukt" -ForegroundColor Red; exit 1 }
Write-Host "    Scoop geinstalleerd" -ForegroundColor Green

Show-Progress -DurationMs 800

# ============================================
#  [2] OPTIMIZATION TOOLS
# ============================================
Write-Host "[2] Apps installeren voor geoptimaliseerde installatie..." -ForegroundColor Yellow

scoop install 7zip
scoop install git
scoop install aria2

Write-Host "    Optimalisatie-apps geinstalleerd" -ForegroundColor Green

Show-Progress -DurationMs 800

# ============================================
#  [3] UV (PYTHON PACKAGE MANAGER)
# ============================================
Write-Host "[3] uv (Python package manager) installeren..." -ForegroundColor Yellow

scoop install uv
if ($LASTEXITCODE -ne 0) { Write-Host "    uv installatie mislukt" -ForegroundColor Red; exit 1 }

uv --version
if ($LASTEXITCODE -ne 0) { Write-Host "    uv niet gevonden op PATH" -ForegroundColor Red; exit 1 }
Write-Host "    uv geinstalleerd: $(uv --version)" -ForegroundColor Green

Show-Progress -DurationMs 800

# ============================================
#  [4] PROJECT DEPENDENCIES
# ============================================
Write-Host "[4] Projectafhankelijkheden synchroniseren..." -ForegroundColor Yellow

# Guard against hardlink issues on OneDrive / VHD mounts
if (-not $env:UV_LINK_MODE) {
    $env:UV_LINK_MODE = "copy"
}

uv sync --extra frontend
if ($LASTEXITCODE -ne 0) { Write-Host "    uv sync mislukt" -ForegroundColor Red; exit 1 }
Write-Host "    Afhankelijkheden gesynchroniseerd" -ForegroundColor Green

Show-Progress -DurationMs 800

# ============================================
#  [5] VERIFICATION
# ============================================
Write-Host "[5] Installatie verifieren..." -ForegroundColor Yellow

uv run python -c "import streamlit; print('streamlit', streamlit.__version__)"
if ($LASTEXITCODE -ne 0) { Write-Host "    Streamlit import mislukt" -ForegroundColor Red; exit 1 }

uv run python -c "import polars; print('polars', polars.__version__)"
if ($LASTEXITCODE -ne 0) { Write-Host "    Polars import mislukt" -ForegroundColor Red; exit 1 }

Write-Host "    Verificatie geslaagd" -ForegroundColor Green

Show-CompletionPrompt
