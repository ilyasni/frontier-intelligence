#Requires -Version 5.1
<#
.SYNOPSIS
    Sync local changes, then switch the server runtime mode over SSH.

.EXAMPLE
    .\scripts\apply-runtime-mode.ps1 full-vision
    .\scripts\apply-runtime-mode.ps1 no-vision -DryRun
    .\scripts\apply-runtime-mode.ps1 gigachat-2-only -NoSync
#>
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("full-vision", "no-vision", "gigachat-2-only")]
    [string] $Mode,

    [string] $RemoteHost = "frontier-intelligence",

    [switch] $DryRun,

    [switch] $NoSync
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not $NoSync) {
    & (Join-Path $ScriptRoot "sync-push.ps1")
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$remoteCmd = "cd /opt/frontier-intelligence && bash scripts/server-apply-runtime-mode.sh $Mode"
if ($DryRun) {
    $remoteCmd += " --dry-run"
}

Write-Host "SSH $RemoteHost -> runtime mode: $Mode" -ForegroundColor Cyan
ssh -o BatchMode=yes $RemoteHost $remoteCmd
if ($LASTEXITCODE -ne 0) {
    Write-Error "Remote runtime mode switch failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "Runtime mode applied: $Mode" -ForegroundColor Green
