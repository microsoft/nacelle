<#
.SYNOPSIS
Compares the current working tree or another Git ref with a captured Criterion baseline.

.EXAMPLE
./scripts/compare-performance.ps1 -BaselineReference v0.3.0

.EXAMPLE
./scripts/compare-performance.ps1 -BaselineReference 00747f3 -CandidateReference HEAD
#>
param(
    [Parameter(Mandatory)][string] $BaselineReference,
    [string] $CandidateReference,
    [ValidateSet("all", "codec", "critical-paths", "telemetry", "response-delivery")]
    [string[]] $Suite = @("all"),
    [string] $OutputDirectory = "target/performance-comparisons"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "performance-common.ps1")

Assert-NacellePerformanceCommand git
Assert-NacellePerformanceCommand cargo
Assert-NacellePerformanceCommand rustc

$baseline = Resolve-NacelleGitReference $BaselineReference
$outputRoot = Resolve-NacellePerformancePath $OutputDirectory
$baselineDirectory = Join-Path $outputRoot (Join-Path "baselines" $baseline.BaselineId)
$baselineMetadataPath = Join-Path $baselineDirectory "metadata.json"
if (-not (Test-Path $baselineMetadataPath)) {
    $captureLogPath = Join-Path $baselineDirectory "capture.log"
    if (Test-Path $captureLogPath) {
        throw "Baseline '$($baseline.BaselineId)' is incomplete. Review '$captureLogPath', then recapture with -Force."
    }
    throw "Baseline '$($baseline.BaselineId)' has not been captured. Run capture-performance-baseline.ps1 first."
}

$baselineMetadata = Get-Content $baselineMetadataPath -Raw | ConvertFrom-Json
$capturedSuites = @($baselineMetadata.suites)
$suiteWasSpecified = $PSBoundParameters.ContainsKey("Suite")
if (-not $suiteWasSpecified) {
    $Suite = $capturedSuites
}
$requestedSuites = @($Suite | Where-Object { $_ -ne "all" })
if ($Suite -contains "all") {
    $requestedSuites = $capturedSuites
    $Suite = $capturedSuites
}
$missingSuites = @($requestedSuites | Where-Object { $capturedSuites -notcontains $_ })
if ($missingSuites.Count -gt 0) {
    throw "Baseline '$($baseline.BaselineId)' does not contain: $($missingSuites -join ', ')."
}

$candidateWorktree = $null
try {
    if ($CandidateReference) {
        $candidate = Resolve-NacelleGitReference $CandidateReference
        $candidateWorktree = New-NacellePerformanceWorktree `
            $candidate.Commit `
            "candidate-$($candidate.ShortCommit)"
        $workspace = $candidateWorktree
        $candidateLabel = $CandidateReference
        $candidateId = $candidate.BaselineId
    }
    else {
        $candidate = Resolve-NacelleGitReference "HEAD"
        $workspace = $script:NacellePerformanceRepoRoot
        $candidateLabel = "working-tree"
        $status = Invoke-NacelleCaptureCommand git @("-C", $workspace, "status", "--porcelain")
        $candidateId = if ([string]::IsNullOrWhiteSpace($status)) {
            $candidate.BaselineId
        }
        else {
            "working-tree-$($candidate.ShortCommit)"
        }
    }

    $timestamp = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
    $comparisonDirectory = Join-Path $outputRoot "comparisons"
    $comparisonName = "$($baseline.BaselineId)-vs-$candidateId-$timestamp"
    $logPath = Join-Path $comparisonDirectory "$comparisonName.log"
    $metadataPath = Join-Path $comparisonDirectory "$comparisonName.json"
    $targetDirectory = Join-Path $outputRoot (Join-Path "cargo-targets" $candidateId)
    Copy-NacelleCriterionBaselines `
        -SourceTargetDirectory $baselineMetadata.criterion_target `
        -DestinationTargetDirectory $targetDirectory

    Invoke-NacellePerformanceBenchmarks `
        -Workspace $workspace `
        -TargetDirectory $targetDirectory `
        -Mode compare `
        -BaselineId $baseline.BaselineId `
        -Suite $Suite `
        -LogPath $logPath

    $candidateMetadata = Get-NacellePerformanceMetadata `
        -Reference $candidateLabel `
        -Commit $candidate.Commit `
        -Workspace $workspace `
        -Suites $requestedSuites
    $comparisonMetadata = [ordered]@{
        compared_at_utc = [DateTime]::UtcNow.ToString("o")
        baseline        = $baselineMetadata
        candidate       = $candidateMetadata
        log             = $logPath
    }
    [System.IO.Directory]::CreateDirectory($comparisonDirectory) | Out-Null
    $comparisonMetadata | ConvertTo-Json -Depth 8 | Set-Content -Path $metadataPath

    Write-Host "==> Compared $candidateLabel with $BaselineReference ($($baseline.BaselineId))"
    Write-Host "==> Log: $logPath"
    Write-Host "==> Metadata: $metadataPath"
}
finally {
    if ($candidateWorktree) {
        Remove-NacellePerformanceWorktree $candidateWorktree
    }
}