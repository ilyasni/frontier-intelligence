#Requires -Version 5.1
<#
.SYNOPSIS
    Sync code to the server, rebuild selected Docker images there, and recreate
    the target services from the freshly built images.

.DESCRIPTION
    Use this for services whose source code is baked into the image with COPY
    (worker, admin, mcp, ingest, crawl4ai, paddleocr, gpt2giga-proxy).
    A plain docker compose restart is not enough after source changes because
    the running container does not mount the repo source tree.

.EXAMPLE
    .\scripts\push-and-remote-deploy.ps1

.EXAMPLE
    .\scripts\push-and-remote-deploy.ps1 -Services worker,admin,mcp
#>
param(
    [string] $RemoteHost = "frontier-intelligence",
    [string[]] $Services = @("worker", "admin"),
    [switch] $NoDelete
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

$syncArgs = @(
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    (Join-Path $ScriptRoot "sync-push.ps1")
)
if ($NoDelete) {
    $syncArgs += "-NoDelete"
}

powershell @syncArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$serviceList = @(
    $Services |
        Where-Object { $_ -and $_.Trim() } |
        ForEach-Object { $_.Trim() }
)
if (-not $serviceList -or $serviceList.Count -eq 0) {
    Write-Error "No services specified."
    exit 1
}

$serviceArgs = $serviceList -join " "
$remoteBuild = @"
cd /opt/frontier-intelligence &&
find scripts -type f -name '*.sh' -print0 | xargs -0 sed -i 's/\r$//' &&
export FRONTIER_PYTHON_BASE_SOURCE=`${FRONTIER_PYTHON_BASE_SOURCE:-python:3.11-slim} &&
export FRONTIER_PYTHON_BASE_MIRROR=`${FRONTIER_PYTHON_BASE_MIRROR:-frontier/python-base:3.11-slim} &&
export COMPOSE_PROFILES=core,ingest,xray,worker,crawl,paddleocr,mcp,admin &&
bash scripts/server-build-stack.sh $serviceArgs &&
docker compose -f docker-compose.yml up -d --force-recreate --wait $serviceArgs
"@

Write-Host "SSH $RemoteHost -> build + recreate: $serviceArgs" -ForegroundColor Cyan
ssh -o BatchMode=yes $RemoteHost $remoteBuild
if ($LASTEXITCODE -ne 0) {
    Write-Error "Remote deploy failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "Remote deploy OK." -ForegroundColor Green
