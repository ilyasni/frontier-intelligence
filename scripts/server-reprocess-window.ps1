# Переобработка окна постов после сбоя провайдера или простоя пайплайна.
#
# ВАЖНО про кодировку: файл обязан лежать в UTF-8 С BOM. PowerShell 5.1 читает
# BOM-less файл как ANSI, и первый же байт кириллицы превращается в «умную кавычку»,
# которая рвёт строку. Так уже не запускался sync-pull.ps1.
#
# Правка 2026-08-04, пункт 44 реестра. Было два независимых дефекта:
#
#   1. Админка закрыта авторизацией (все /api/* кроме /api/health и /api/auth/login
#      отвечают 401), а скрипт ходил без единого credential. Боевой прогон не
#      переобрабатывал НИЧЕГО.
#   2. Хуже первого: результат вызова не проверялся вообще. Строка была
#      `ssh $Server "curl -fsS -X POST ..." | Out-Null`, а в PowerShell 5.1 ненулевой
#      код возврата нативного exe НЕ бросает исключение даже под
#      $ErrorActionPreference = 'Stop'. Поэтому «reprocess ok N/N» печаталось
#      независимо от 401, 500 и таймаута.
#
# Второй дефект тут главный: одна авторизация вылечила бы сегодняшний симптом
# и оставила механизм молчания на месте.
#
# Учётные данные с этой машины не передаются: их читает серверная половина
# (scripts/reprocess-window.sh) из серверного .env.

param(
    [Parameter(Mandatory = $true)]
    [string]$Server,

    [Parameter(Mandatory = $true)]
    [string]$StartUtc,

    [Parameter(Mandatory = $true)]
    [string]$EndUtc,

    [string]$Status = "dropped",
    [int]$DelayMs = 200,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Assert-LastExitCode {
    param(
        [Parameter(Mandatory = $true)]
        [string]$What
    )

    if ($LASTEXITCODE -ne 0) {
        throw "$What failed with exit code $LASTEXITCODE"
    }
}

function Invoke-RemotePsql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Sql
    )

    $out = $Sql | ssh $Server "docker exec -i frontier-intelligence-postgres-1 psql -U frontier -d frontier -At"
    Assert-LastExitCode -What "remote psql"
    return $out
}

$sql = @"
SELECT p.id::text
FROM posts p
JOIN indexing_status i ON i.post_id = p.id
WHERE i.updated_at >= TIMESTAMPTZ '$StartUtc'
  AND i.updated_at <  TIMESTAMPTZ '$EndUtc'
  AND i.embedding_status = '$Status'
ORDER BY i.updated_at, p.id;
"@

$ids = @(Invoke-RemotePsql -Sql $sql | Where-Object { $_ -and $_.Trim() })

Write-Output "server=$Server start=$StartUtc end=$EndUtc status=$Status count=$($ids.Count) dry_run=$($DryRun.IsPresent)"

if ($ids.Count -eq 0) {
    return
}

if ($DryRun) {
    $ids
    return
}

# Один удалённый сеанс на всё окно вместо отдельного ssh-рукопожатия на каждый пост.
# Список id уходит на stdin, авторизация и пауза — на серверной стороне.
$delaySeconds = [math]::Round($DelayMs / 1000.0, 3)
$remote = "bash /opt/frontier-intelligence/scripts/reprocess-window.sh $delaySeconds"

$ids -join "`n" | ssh $Server $remote
Assert-LastExitCode -What "remote reprocess"

Write-Output "replayed=$($ids.Count)"
