#Requires -Version 5.1
<#
.SYNOPSIS
    Simulate OneDrive Known Folder Move (KFM) by creating a VHD and
    redirecting the Documents and Downloads shell folders to it.

    This reproduces the filesystem-boundary conditions that institutional
    OneDrive deployments create, where the user profile lives on C: but
    known folders are on a different volume backed by OneDrive.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step { param([string]$Msg) Write-Host "[ONEDRIVE-SIM]  $Msg" -ForegroundColor Magenta }

$vhdPath   = "C:\odsim.vhdx"
$vhdSizeGB = 2
$driveLetter = "O"
$mountRoot = "${driveLetter}:\OneDriveSync"

# ── 1. Create and mount a VHD ─────────────────────────────────────────────────
Write-Step "Creating ${vhdSizeGB}GB VHD at $vhdPath"

$diskpartScript = @"
create vdisk file="$vhdPath" maximum=$($vhdSizeGB * 1024) type=expandable
select vdisk file="$vhdPath"
attach vdisk
create partition primary
format fs=ntfs label="OneDriveSim" quick
assign letter=$driveLetter
"@

$diskpartFile = Join-Path $env:TEMP "odsim-diskpart.txt"
$diskpartScript | Set-Content -Path $diskpartFile -Encoding ASCII

diskpart /s $diskpartFile
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL]  diskpart failed" -ForegroundColor Red
    exit 1
}

# Wait briefly for the volume to become available
Start-Sleep -Seconds 2

if (-not (Test-Path "${driveLetter}:\")) {
    Write-Host "[FAIL]  Drive ${driveLetter}: did not mount" -ForegroundColor Red
    exit 1
}
Write-Step "VHD mounted as ${driveLetter}:"

# ── 2. Create folder structure ─────────────────────────────────────────────────
$simDocuments = Join-Path $mountRoot "Documents"
$simDownloads = Join-Path $mountRoot "Downloads"

New-Item -ItemType Directory -Path $simDocuments -Force | Out-Null
New-Item -ItemType Directory -Path $simDownloads -Force | Out-Null
Write-Step "Created $simDocuments and $simDownloads"

# ── 3. Redirect known folders via registry ─────────────────────────────────────
# These are the same registry keys that OneDrive KFM modifies.
$shellFolders     = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders"
$knownFolderPaths = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders"

# Documents  {F42EE2D3-909F-4907-8871-4C22FC0BF756} / Personal
Set-ItemProperty -Path $shellFolders     -Name "Personal"    -Value $simDocuments
Set-ItemProperty -Path $shellFolders     -Name "{F42EE2D3-909F-4907-8871-4C22FC0BF756}" -Value $simDocuments
Set-ItemProperty -Path $knownFolderPaths -Name "Personal"    -Value $simDocuments

# Downloads  {374DE290-123F-4565-9164-39C4925E467B} / {7D83EE9B-2244-4E70-B1F5-5393042AF1E4}
Set-ItemProperty -Path $shellFolders     -Name "{374DE290-123F-4565-9164-39C4925E467B}" -Value $simDownloads
Set-ItemProperty -Path $knownFolderPaths -Name "{374DE290-123F-4565-9164-39C4925E467B}" -Value $simDownloads

Write-Step "Known folders redirected to VHD"

# ── 4. Export env vars for later steps ─────────────────────────────────────────
# Make the simulated paths available to subsequent workflow steps
if ($env:GITHUB_ENV) {
    Add-Content -Path $env:GITHUB_ENV -Value "ONEDRIVE_SIM_ROOT=$mountRoot"
    Add-Content -Path $env:GITHUB_ENV -Value "ONEDRIVE_SIM_DOCUMENTS=$simDocuments"
    Add-Content -Path $env:GITHUB_ENV -Value "ONEDRIVE_SIM_DOWNLOADS=$simDownloads"
}

Write-Host "`n[DONE]  OneDrive simulation ready" -ForegroundColor Green
Write-Host "        Documents -> $simDocuments"
Write-Host "        Downloads -> $simDownloads"
