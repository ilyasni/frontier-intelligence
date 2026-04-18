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

function Invoke-RemotePsql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Sql
    )

    $Sql | ssh $Server "docker exec -i frontier-intelligence-postgres-1 psql -U frontier -d frontier -At"
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

$done = 0
foreach ($id in $ids) {
    ssh $Server "curl -fsS -X POST http://127.0.0.1:8101/api/pipeline/reprocess/$id" | Out-Null
    $done += 1
    Write-Output "reprocess ok $done/$($ids.Count) $id"
    Start-Sleep -Milliseconds $DelayMs
}

Write-Output "replayed=$done"
