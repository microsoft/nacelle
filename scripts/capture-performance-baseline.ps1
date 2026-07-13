<#
.SYNOPSIS
Captures Criterion baselines for the current commit, a Git tag, or another commit.

.EXAMPLE
./scripts/capture-performance-baseline.ps1

.EXAMPLE
./scripts/capture-performance-baseline.ps1 -Reference v0.3.0

.EXAMPLE
./scripts/capture-performance-baseline.ps1 -Reference 00747f3 -Suite critical-paths,telemetry
#>
param(
    [string] $Reference = "HEAD",
    [ValidateSet("all", "codec", "critical-paths", "telemetry", "response-delivery")]
    [string[]] $Suite = @("all"),
    [string] $OutputDirectory = "target/performance-comparisons",
    [switch] $Force
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "performance-common.ps1")

Assert-NacellePerformanceCommand git
Assert-NacellePerformanceCommand cargo
Assert-NacellePerformanceCommand rustc

$resolved = Resolve-NacelleGitReference $Reference
$outputRoot = Resolve-NacellePerformancePath $OutputDirectory
$targetDirectory = Join-Path $outputRoot (Join-Path "cargo-targets" $resolved.BaselineId)
$baselineDirectory = Join-Path $outputRoot (Join-Path "baselines" $resolved.BaselineId)
$metadataPath = Join-Path $baselineDirectory "metadata.json"
$logPath = Join-Path $baselineDirectory "capture.log"

if ((Test-Path $baselineDirectory) -and -not $Force) {
    if (Test-Path $metadataPath) {
        throw "Baseline '$($resolved.BaselineId)' already exists. Use -Force to replace it."
    }
    throw "Baseline '$($resolved.BaselineId)' is incomplete. Review its capture.log, then use -Force to replace it."
}

if ($Force -and (Test-Path $baselineDirectory)) {
    Remove-Item $baselineDirectory -Recurse -Force
}
[System.IO.Directory]::CreateDirectory($baselineDirectory) | Out-Null
if ($Force) {
    Remove-NacelleCriterionBaseline $targetDirectory $resolved.BaselineId
}

$worktree = $null
try {
    $worktree = New-NacellePerformanceWorktree $resolved.Commit "baseline-$($resolved.ShortCommit)"
    Invoke-NacellePerformanceBenchmarks `
        -Workspace $worktree `
        -TargetDirectory $targetDirectory `
        -Mode capture `
        -BaselineId $resolved.BaselineId `
        -Suite $Suite `
        -LogPath $logPath

    $metadata = Get-NacellePerformanceMetadata `
        -Reference $Reference `
        -Commit $resolved.Commit `
        -Workspace $worktree `
        -Suites (Get-NacellePerformanceBenchmarks -Suite $Suite -Workspace $worktree).Name
    $metadata["baseline_id"] = $resolved.BaselineId
    $metadata["criterion_target"] = $targetDirectory
    $metadata | ConvertTo-Json -Depth 5 | Set-Content -Path $metadataPath
}
finally {
    if ($worktree) {
        Remove-NacellePerformanceWorktree $worktree
    }
}

Write-Host "==> Captured $Reference as $($resolved.BaselineId)"
Write-Host "==> Metadata: $metadataPath"