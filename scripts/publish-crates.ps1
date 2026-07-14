param(
    [switch]$Publish,
    [switch]$SkipValidation,
    [switch]$AllowDirty,
    [int]$IndexWaitSeconds = 300,
    [int]$PollSeconds = 15
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -Path $repoRoot

function Invoke-Step {
    param(
        [string] $Name,
        [scriptblock] $Command
    )

    Write-Host "==> $Name"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE"
    }
}

function Assert-CleanWorkingTree {
    git diff --quiet
    if ($LASTEXITCODE -ne 0) {
        throw "Working tree has unstaged changes. Commit them or pass -AllowDirty."
    }

    git diff --cached --quiet
    if ($LASTEXITCODE -ne 0) {
        throw "Working tree has staged changes. Commit them or pass -AllowDirty."
    }

    $untracked = git ls-files --others --exclude-standard
    if ($untracked) {
        throw "Working tree has untracked files. Commit them or pass -AllowDirty."
    }
}

function Get-PackageVersion {
    param([string] $Name)

    $metadata = cargo metadata --no-deps --format-version 1 | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw "cargo metadata failed with exit code $LASTEXITCODE"
    }

    $package = $metadata.packages | Where-Object { $_.name -eq $Name } | Select-Object -First 1
    if (-not $package) {
        throw "Package '$Name' was not found in cargo metadata"
    }

    return $package.version
}

function Test-CrateVersion {
    param(
        [string] $Name,
        [string] $Version
    )

    $uri = "https://crates.io/api/v1/crates/$Name/$Version"
    try {
        Invoke-RestMethod `
            -Uri $uri `
            -Headers @{ "User-Agent" = "nacelle-publish-script" } `
            -TimeoutSec 20 | Out-Null
        return $true
    }
    catch {
        if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 404) {
            return $false
        }

        throw
    }
}

function Wait-CrateVersion {
    param(
        [string] $Name,
        [string] $Version
    )

    $deadline = (Get-Date).AddSeconds($IndexWaitSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-CrateVersion -Name $Name -Version $Version) {
            Write-Host "==> crates.io has $Name $Version"
            return
        }

        Write-Host "==> Waiting for crates.io index: $Name $Version"
        Start-Sleep -Seconds $PollSeconds
    }

    throw "Timed out waiting for crates.io to expose $Name $Version"
}

function Invoke-CargoPublish {
    param(
        [string] $Name,
        [string] $Version
    )

    if ($Publish -and (Test-CrateVersion -Name $Name -Version $Version)) {
        Write-Warning "$Name $Version already exists on crates.io; skipping"
        return
    }

    $cargoArgs = @("publish", "-p", $Name)
    if (-not $Publish) {
        $cargoArgs += "--dry-run"
    }
    if ($AllowDirty) {
        $cargoArgs += "--allow-dirty"
    }

    Invoke-Step "cargo $($cargoArgs -join ' ')" { cargo @cargoArgs }
}

function Invoke-CargoPackageList {
    param([string] $Name)

    $cargoArgs = @("package", "-p", $Name, "--list")
    if ($AllowDirty) {
        $cargoArgs += "--allow-dirty"
    }

    Invoke-Step "cargo $($cargoArgs -join ' ')" { cargo @cargoArgs | Out-Null }
}

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "cargo was not found on PATH"
}

if (-not $AllowDirty) {
    Assert-CleanWorkingTree
}

if (-not $SkipValidation) {
    Invoke-Step "validate all" { & (Join-Path $PSScriptRoot "validate-all.ps1") }
}

$release = [ordered]@{
    "nacelle-codec"   = Get-PackageVersion -Name "nacelle-codec"
    "nacelle-core"    = Get-PackageVersion -Name "nacelle-core"
    "nacelle-openssl" = Get-PackageVersion -Name "nacelle-openssl"
    "nacelle-rustls"  = Get-PackageVersion -Name "nacelle-rustls"
    "nacelle-tcp"     = Get-PackageVersion -Name "nacelle-tcp"
    "nacelle-http"    = Get-PackageVersion -Name "nacelle-http"
    "nacelle"         = Get-PackageVersion -Name "nacelle"
}

if (-not $Publish) {
    Invoke-CargoPublish -Name "nacelle-codec" -Version $release["nacelle-codec"]
    Invoke-CargoPublish -Name "nacelle-core" -Version $release["nacelle-core"]
    Invoke-CargoPackageList -Name "nacelle-openssl"
    Invoke-CargoPackageList -Name "nacelle-rustls"
    Invoke-CargoPackageList -Name "nacelle-tcp"
    Invoke-CargoPackageList -Name "nacelle-http"
    Invoke-CargoPackageList -Name "nacelle"
    Write-Host "==> Dry run verified nacelle-codec and nacelle-core."
    Write-Host "==> Package payloads validated for provider, transport, and facade crates."
    Write-Host "==> Cargo can fully verify dependent crates after their $($release['nacelle-core']) internal dependencies reach crates.io."
    Write-Host "==> Run with -Publish to publish in dependency order."
    return
}

Invoke-CargoPublish -Name "nacelle-codec" -Version $release["nacelle-codec"]
Wait-CrateVersion -Name "nacelle-codec" -Version $release["nacelle-codec"]

Invoke-CargoPublish -Name "nacelle-core" -Version $release["nacelle-core"]
Wait-CrateVersion -Name "nacelle-core" -Version $release["nacelle-core"]

Invoke-CargoPublish -Name "nacelle-openssl" -Version $release["nacelle-openssl"]
Invoke-CargoPublish -Name "nacelle-rustls" -Version $release["nacelle-rustls"]
Wait-CrateVersion -Name "nacelle-openssl" -Version $release["nacelle-openssl"]
Wait-CrateVersion -Name "nacelle-rustls" -Version $release["nacelle-rustls"]

Invoke-CargoPublish -Name "nacelle-tcp" -Version $release["nacelle-tcp"]
Invoke-CargoPublish -Name "nacelle-http" -Version $release["nacelle-http"]
Wait-CrateVersion -Name "nacelle-tcp" -Version $release["nacelle-tcp"]
Wait-CrateVersion -Name "nacelle-http" -Version $release["nacelle-http"]

Invoke-CargoPublish -Name "nacelle" -Version $release["nacelle"]
Wait-CrateVersion -Name "nacelle" -Version $release["nacelle"]

Write-Host "==> Published nacelle crate family"
