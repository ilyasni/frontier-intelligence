#Requires -Version 5.1
<#
.SYNOPSIS
    rsync push (WSL) + SSH: SQL-миграции в Postgres-контейнере и restart worker/ingest/crawl4ai/admin.

.EXAMPLE
    .\scripts\push-and-remote-migrate.ps1
    .\scripts\push-and-remote-migrate.ps1 -RemoteHost "user@host"
#>
param(
    [string] $RemoteHost = "frontier-intelligence"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
& (Join-Path $ScriptRoot "sync-push.ps1")
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$remoteCmd = 'cd /opt/frontier-intelligence && bash scripts/server-apply-sql-migrations.sh'
Write-Host "SSH $RemoteHost -> migrate + restart..." -ForegroundColor Cyan
ssh -o BatchMode=yes $RemoteHost $remoteCmd
if ($LASTEXITCODE -ne 0) {
    Write-Error "SSH remote migrate failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}
Write-Host "Remote migrate OK." -ForegroundColor Green
