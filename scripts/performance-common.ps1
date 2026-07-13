$ErrorActionPreference = "Stop"

$script:NacellePerformanceRepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$script:NacellePerformanceBenchmarks = @(
    [pscustomobject]@{
        Name      = "codec"
        Arguments = @("bench", "-p", "nacelle-codec", "--bench", "framed_comparison", "--all-features")
    },
    [pscustomobject]@{
        Name      = "critical-paths"
        Arguments = @("bench", "-p", "nacelle-examples", "--bench", "critical_paths", "--features", "bench tcp")
    },
    [pscustomobject]@{
        Name      = "telemetry"
        Arguments = @("bench", "-p", "nacelle-tcp", "--bench", "telemetry_paths", "--all-features")
    },
    [pscustomobject]@{
        Name      = "response-delivery"
        Arguments = @("bench", "-p", "nacelle-tcp", "--bench", "response_delivery")
    }
)

function Assert-NacellePerformanceCommand {
    param([Parameter(Mandatory)][string] $Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found on PATH."
    }
}

function Invoke-NacelleCaptureCommand {
    param(
        [Parameter(Mandatory)][string] $Command,
        [Parameter(Mandatory)][string[]] $Arguments
    )

    $output = & $Command @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "$Command $($Arguments -join ' ') failed:`n$($output -join [Environment]::NewLine)"
    }
    return ($output | Out-String).Trim()
}

function Resolve-NacellePerformancePath {
    param([Parameter(Mandatory)][string] $Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $script:NacellePerformanceRepoRoot $Path))
}

function Resolve-NacelleGitReference {
    param([Parameter(Mandatory)][string] $Reference)

    $commit = Invoke-NacelleCaptureCommand git @(
        "-C", $script:NacellePerformanceRepoRoot,
        "rev-parse", "--verify", "${Reference}^{commit}"
    )
    $shortCommit = Invoke-NacelleCaptureCommand git @(
        "-C", $script:NacellePerformanceRepoRoot,
        "rev-parse", "--short=12", $commit
    )
    return [pscustomobject]@{
        Reference   = $Reference
        Commit      = $commit
        ShortCommit = $shortCommit
        BaselineId  = "commit-$shortCommit"
    }
}

function Get-NacellePerformanceBenchmarks {
    param([Parameter(Mandatory)][string[]] $Suite)

    if ($Suite -contains "all") {
        return $script:NacellePerformanceBenchmarks
    }

    $selected = @($script:NacellePerformanceBenchmarks | Where-Object { $Suite -contains $_.Name })
    if ($selected.Count -ne $Suite.Count) {
        $known = $script:NacellePerformanceBenchmarks.Name -join ", "
        throw "Unknown benchmark suite. Available suites: all, $known"
    }
    return $selected
}

function New-NacellePerformanceWorktree {
    param(
        [Parameter(Mandatory)][string] $Commit,
        [Parameter(Mandatory)][string] $Label
    )

    $root = Join-Path ([System.IO.Path]::GetTempPath()) "nacelle-performance-worktrees"
    [System.IO.Directory]::CreateDirectory($root) | Out-Null
    $path = Join-Path $root "$Label-$PID-$([Guid]::NewGuid().ToString('N'))"
    & git -C $script:NacellePerformanceRepoRoot worktree add --detach $path $Commit | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to create temporary worktree for $Commit."
    }
    return $path
}

function Remove-NacellePerformanceWorktree {
    param([Parameter(Mandatory)][string] $Path)

    & git -C $script:NacellePerformanceRepoRoot worktree remove --force $Path | Out-Host
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Unable to remove temporary worktree '$Path'."
    }
}

function Remove-NacelleCriterionBaseline {
    param(
        [Parameter(Mandatory)][string] $TargetDirectory,
        [Parameter(Mandatory)][string] $BaselineId
    )

    $criterionRoot = Join-Path $TargetDirectory "criterion"
    if (-not (Test-Path $criterionRoot)) {
        return
    }

    Get-ChildItem $criterionRoot -Directory -Recurse |
    Where-Object { $_.Name -eq $BaselineId } |
    Sort-Object { $_.FullName.Length } -Descending |
    Remove-Item -Recurse -Force
}

function Invoke-NacellePerformanceBenchmarks {
    param(
        [Parameter(Mandatory)][string] $Workspace,
        [Parameter(Mandatory)][string] $TargetDirectory,
        [Parameter(Mandatory)][ValidateSet("capture", "compare")][string] $Mode,
        [Parameter(Mandatory)][string] $BaselineId,
        [Parameter(Mandatory)][string[]] $Suite,
        [Parameter(Mandatory)][string] $LogPath
    )

    $benchmarks = Get-NacellePerformanceBenchmarks $Suite
    $criterionOption = if ($Mode -eq "capture") { "--save-baseline" } else { "--baseline" }
    $previousTargetDirectory = $env:CARGO_TARGET_DIR
    $previousLocation = Get-Location
    [System.IO.Directory]::CreateDirectory((Split-Path -Parent $LogPath)) | Out-Null
    Set-Content -Path $LogPath -Value "Nacelle performance $Mode for $BaselineId"

    try {
        $env:CARGO_TARGET_DIR = $TargetDirectory
        Set-Location $Workspace
        foreach ($benchmark in $benchmarks) {
            Write-Host "==> $Mode $($benchmark.Name)"
            Add-Content -Path $LogPath -Value "`n==> $($benchmark.Name)"
            $arguments = @($benchmark.Arguments) + @("--", $criterionOption, $BaselineId, "--noplot")
            & cargo @arguments 2>&1 | Tee-Object -FilePath $LogPath -Append
            if ($LASTEXITCODE -ne 0) {
                throw "Benchmark '$($benchmark.Name)' failed with exit code $LASTEXITCODE."
            }
        }
    }
    finally {
        Set-Location $previousLocation
        $env:CARGO_TARGET_DIR = $previousTargetDirectory
    }
}

function Get-NacellePerformanceMetadata {
    param(
        [Parameter(Mandatory)][string] $Reference,
        [Parameter(Mandatory)][string] $Commit,
        [Parameter(Mandatory)][string] $Workspace,
        [Parameter(Mandatory)][string[]] $Suites
    )

    $status = Invoke-NacelleCaptureCommand git @("-C", $Workspace, "status", "--porcelain")
    $cpu = $env:PROCESSOR_IDENTIFIER
    if (-not $cpu -and (Get-Command lscpu -ErrorAction SilentlyContinue)) {
        $cpu = Invoke-NacelleCaptureCommand lscpu @()
    }

    return [ordered]@{
        captured_at_utc  = [DateTime]::UtcNow.ToString("o")
        reference        = $Reference
        commit           = $Commit
        dirty            = -not [string]::IsNullOrWhiteSpace($status)
        suites           = @($Suites)
        rustc            = Invoke-NacelleCaptureCommand rustc @("-Vv")
        cargo            = Invoke-NacelleCaptureCommand cargo @("-V")
        operating_system = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        architecture     = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        cpu              = $cpu
    }
}