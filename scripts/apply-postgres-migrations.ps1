# Применить storage/postgres/migrations/*.sql по порядку (нужен psql в PATH).
# Пример: $env:DATABASE_URL = "postgresql://user:pass@localhost:5432/frontier"; .\scripts\apply-postgres-migrations.ps1

param(
    [string] $DatabaseUrl = $env:DATABASE_URL
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Migrations = Join-Path $Root "storage\postgres\migrations"

if ([string]::IsNullOrWhiteSpace($DatabaseUrl)) {
    Write-Error "Задайте DATABASE_URL или передайте -DatabaseUrl"
}

$psqlUrl = $DatabaseUrl -replace "\+asyncpg", ""

Get-ChildItem -Path $Migrations -Filter "*.sql" | Sort-Object Name | ForEach-Object {
    Write-Host "==> $($_.FullName)"
    & psql $psqlUrl -v ON_ERROR_STOP=1 -f $_.FullName
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
Write-Host "Migrations OK"
