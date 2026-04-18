#Requires -Version 5.1
<#
.SYNOPSIS
    Push локального кода на удалённый сервер frontier-intelligence через rsync/WSL.

.PARAMETER DryRun
    Показать что будет передано без реальных изменений.

.PARAMETER NoDelete
    Безопасный режим: только добавлять/обновлять, не удалять файлы на сервере.

.EXAMPLE
    .\sync-push.ps1                  # обычный push
    .\sync-push.ps1 -DryRun          # preview
    .\sync-push.ps1 -NoDelete        # только обновить, не удалять
#>
param(
    [switch]$DryRun,
    [switch]$NoDelete
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── Конфиг ────────────────────────────────────────────────────────
$RemoteHost  = "frontier-intelligence"
$RemotePath  = "/opt/frontier-intelligence/"
$LocalPath   = Split-Path $PSScriptRoot -Parent   # scripts/ → корень проекта
$ExcludeFile = Join-Path $LocalPath ".rsync-exclude"

# ── Проверка WSL (stderr proxy warning не считаем фатальным) ───────
$prevEap = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$null = wsl echo ok 2>&1
$wslExit = $LASTEXITCODE
$ErrorActionPreference = $prevEap
if ($wslExit -ne 0) {
    Write-Error "WSL not available or failed (exit $wslExit). Install: wsl --install -d Ubuntu"
    exit 1
}

# ── Конвертация Windows пути в WSL (wslpath: /mnt/host/d/... при Docker Desktop WSL2) ──
function ConvertTo-WslPath([string]$WinPath) {
    $unixish = $WinPath -replace '\\', '/'
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $raw = wsl sh -c "wslpath -a '$unixish'" 2>&1
    $ErrorActionPreference = $prev
    foreach ($line in $raw) {
        $t = "$line".Trim()
        if ($t.StartsWith('/')) {
            return $t
        }
    }
    $drive = $WinPath.Substring(0, 1).ToLower()
    $rest  = $WinPath.Substring(2).Replace('\', '/')
    return "/mnt/$drive$rest"
}

$WslLocalPath   = ConvertTo-WslPath $LocalPath
$WslExcludeFile = ConvertTo-WslPath $ExcludeFile

if (-not (Test-Path -LiteralPath $ExcludeFile)) {
    Write-Error "Exclude file not found: $ExcludeFile"
    exit 1
}

# В WSL часто нет `ssh` в PATH — используем Windows OpenSSH изнутри WSL
$SshWin = Join-Path $env:WINDIR "System32\OpenSSH\ssh.exe"
$WslSshExe = $null
if (Test-Path -LiteralPath $SshWin) {
    $WslSshExe = ConvertTo-WslPath $SshWin
}

# ── Сборка аргументов rsync ───────────────────────────────────────
$args = @("-avz")
if ($WslSshExe) {
    $args += @("-e", $WslSshExe)
}
$args += @(
    "--exclude-from=$WslExcludeFile",
    "--no-perms",
    "--chmod=Du=rwx,Dg=rwx,Fu=rw,Fg=rw"
)

if ($DryRun) {
    $args += "--dry-run"
    Write-Host "[DRY-RUN] Изменений не будет. Только просмотр." -ForegroundColor Yellow
}

if (-not $NoDelete -and -not $DryRun) {
    $args += "--delete"
    # --delete-excluded намеренно НЕ используется:
    # он удалял бы storage/neo4j/, sessions/, .env с сервера
}

$args += "$WslLocalPath/"
$args += "${RemoteHost}:${RemotePath}"

# ── Запуск ────────────────────────────────────────────────────────
Write-Host ""
Write-Host "PUSH: $LocalPath" -ForegroundColor Cyan
Write-Host "  →   ${RemoteHost}:${RemotePath}" -ForegroundColor Cyan
Write-Host ""
Write-Host "rsync $($args -join ' ')" -ForegroundColor DarkGray
Write-Host ""

wsl rsync @args

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "rsync push OK." -ForegroundColor Green
} else {
    Write-Host ""
    Write-Error "rsync failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}
