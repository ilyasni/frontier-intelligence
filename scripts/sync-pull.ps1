#Requires -Version 5.1
<#
.SYNOPSIS
    Pull кода с удалённого сервера frontier-intelligence на локальный Windows.
    Для первоначальной синхронизации или обновления.
    НИКОГДА не использует --delete — .cursor/, AGENTS.md и CLAUDE.md остаются в безопасности.

.PARAMETER DryRun
    Показать что изменится без реальных изменений.

.EXAMPLE
    .\sync-pull.ps1            # получить код с сервера
    .\sync-pull.ps1 -DryRun    # preview
#>
param(
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── Конфиг ────────────────────────────────────────────────────────
$RemoteHost  = "frontier-intelligence"
$RemotePath  = "/opt/frontier-intelligence/"
$LocalPath   = Split-Path $PSScriptRoot -Parent
$ExcludeFile = Join-Path $LocalPath ".rsync-exclude"

# ── Проверка WSL (stderr proxy warning не фатален) ─────────────────
$prevEap = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
$null = wsl echo ok 2>&1
$wslExit = $LASTEXITCODE
$ErrorActionPreference = $prevEap
if ($wslExit -ne 0) {
    Write-Error "WSL not available (exit $wslExit). Install: wsl --install -d Ubuntu"
    exit 1
}

# ── Конвертация Windows пути в WSL (wslpath, см. sync-push.ps1) ───
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

$SshWin = Join-Path $env:WINDIR "System32\OpenSSH\ssh.exe"
$WslSshExe = $null
if (Test-Path -LiteralPath $SshWin) {
    $WslSshExe = ConvertTo-WslPath $SshWin
}

# ── Сборка аргументов rsync ───────────────────────────────────────
$rsyncArgs = @("-avz")
if ($WslSshExe) {
    $rsyncArgs += @("-e", $WslSshExe)
}
$rsyncArgs += @(
    "--exclude-from=$WslExcludeFile",
    "--no-perms",
    # --delete намеренно НЕ используется: локальные .cursor/, AGENTS.md, CLAUDE.md, .vscode/ не трогаем
)

if ($DryRun) {
    $rsyncArgs += "--dry-run"
    Write-Host "[DRY-RUN] Изменений не будет. Только просмотр." -ForegroundColor Yellow
}

$rsyncArgs += "${RemoteHost}:${RemotePath}"
$rsyncArgs += "$WslLocalPath/"

# ── Запуск ────────────────────────────────────────────────────────
Write-Host ""
Write-Host "PULL: ${RemoteHost}:${RemotePath}" -ForegroundColor Cyan
Write-Host "  →   $LocalPath" -ForegroundColor Cyan
Write-Host ""
Write-Host "Примечание: --delete НЕ используется. Локальные файлы (.cursor/, AGENTS.md, CLAUDE.md) в безопасности." -ForegroundColor DarkYellow
Write-Host ""
Write-Host "rsync $($rsyncArgs -join ' ')" -ForegroundColor DarkGray
Write-Host ""

wsl rsync @rsyncArgs

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "rsync pull OK." -ForegroundColor Green
    if (-not $DryRun) {
        Write-Host ""
        Write-Host "Если есть .sh скрипты — восстановите +x на сервере:" -ForegroundColor DarkYellow
        Write-Host "  ssh $RemoteHost `"find /opt/frontier-intelligence -name '*.sh' -exec chmod +x {} \;`"" -ForegroundColor DarkGray
    }
} else {
    Write-Host ""
    Write-Error "rsync failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}
