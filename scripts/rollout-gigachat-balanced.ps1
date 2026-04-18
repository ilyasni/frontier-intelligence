param(
    [string]$BaseEnvFile = ".env",
    [string]$OverlayEnvFile = ".env.balanced.example",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Import-EnvFile {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Env file not found: $Path"
    }

    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            return
        }
        $pair = $line -split "=", 2
        if ($pair.Count -ne 2) {
            return
        }
        $key = $pair[0].Trim()
        $value = $pair[1]
        if ($key) {
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
}

function Assert-RequiredEnv {
    param([string[]]$Names)

    $missing = @()
    foreach ($name in $Names) {
        $value = [Environment]::GetEnvironmentVariable($name, "Process")
        if ([string]::IsNullOrWhiteSpace($value)) {
            $missing += $name
        }
    }
    if ($missing.Count -gt 0) {
        throw "Missing required env values: $($missing -join ', ')"
    }
}

Import-EnvFile -Path $BaseEnvFile
Import-EnvFile -Path $OverlayEnvFile

Assert-RequiredEnv -Names @(
    "POSTGRES_PASSWORD",
    "NEO4J_PASSWORD",
    "GIGACHAT_CREDENTIALS"
)

$cmd = @(
    "docker", "compose",
    "--profile", "core",
    "--profile", "worker",
    "--profile", "mcp",
    "up", "-d", "--force-recreate",
    "gpt2giga-proxy", "worker", "mcp"
)

Write-Host "Balanced rollout command:" -ForegroundColor Cyan
Write-Host ($cmd -join " ")

if ($DryRun) {
    exit 0
}

& $cmd[0] $cmd[1..($cmd.Length - 1)]
