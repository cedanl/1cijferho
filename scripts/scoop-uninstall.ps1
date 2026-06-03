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
    $options = @("Verwijdering Starten", "Afsluiten")
    $selected = 0

    do {
        Clear-Host
        Write-Host ""
        Write-Host " ===================================" -ForegroundColor Red
        Write-Host " 1CIJFERHO UNINSTALL" -ForegroundColor Red
        Write-Host " ===================================" -ForegroundColor Red
        Write-Host ""
        Write-Host "LET OP: Gebruik dit hulpmiddel alleen voor het verwijderen van Scoop en alle geinstalleerde afhankelijkheden." -ForegroundColor Yellow
        Write-Host ""
        Write-Host " Dit script zal:" -ForegroundColor Gray
        Write-Host "  * De virtuele omgeving (.venv) verwijderen" -ForegroundColor Gray
        Write-Host "  * Scoop, buckets & alle applicaties verwijderen" -ForegroundColor Gray
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

    # Confirmation prompt
    Write-Host "Wil je doorgaan met het verwijderen? " -NoNewline -ForegroundColor Red
    Write-Host -NoNewline "(J/n) " -ForegroundColor Cyan
    $response = Read-Host
    if ($response -ne "j") {
        Write-Host " Afsluiten..." -ForegroundColor Yellow
        exit 0
    }

    Write-Host "Weet je het zeker? Dit kan niet ongedaan worden gemaakt. " -NoNewline -ForegroundColor Red
    Write-Host -NoNewline "(J/n) " -ForegroundColor Cyan
    $response = Read-Host
    if ($response -ne "j") {
        Write-Host " Afsluiten..." -ForegroundColor Yellow
        exit 0
    }
}

# ============================================
#  [1] REMOVE VIRTUAL ENVIRONMENT
# ============================================
Write-Host "[1] Virtuele omgeving verwijderen..." -ForegroundColor Yellow

if (Test-Path ".venv") {
    Remove-Item -Recurse -Force ".venv"
    Write-Host "    .venv verwijderd" -ForegroundColor Green
} else {
    Write-Host "    .venv niet gevonden, stap overgeslagen..." -ForegroundColor Gray
}

Show-Progress -DurationMs 800

# ============================================
#  [2] REMOVE SCOOP & ALL APPLICATIONS
# ============================================
Write-Host "[2] Scoop, buckets & applicaties verwijderen..." -ForegroundColor Yellow

if (Get-Command scoop -ErrorAction SilentlyContinue) {
    scoop uninstall scoop
    Write-Host "    Scoop verwijderd" -ForegroundColor Green
} else {
    Write-Host "    Scoop is al verwijderd, stap overgeslagen..." -ForegroundColor Gray
}

Show-Progress -DurationMs 800

# ============================================
#  END
# ============================================
Show-CompletionPrompt
