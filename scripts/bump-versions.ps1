<#
.SYNOPSIS
Updates the coordinated Nacelle crate versions.

.DESCRIPTION
Major, minor, and patch actions bump the numeric version. For dev and rc
versions, patch advances the prerelease counter while major and minor start a
new counter at 1. Use promote-rc and promote-release for stage promotions, and
start-dev to begin a new dev cycle from a release.

.PARAMETER Action
One of major, minor, patch, promote-rc, promote-release, or start-dev.

.PARAMETER Bump
The major or minor component to bump when starting a dev cycle. Defaults to minor.

.EXAMPLE
./scripts/bump-versions.ps1 -Action patch

.EXAMPLE
./scripts/bump-versions.ps1 -Action promote-rc

.EXAMPLE
./scripts/bump-versions.ps1 -Action start-dev -Bump minor
#>
[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "Medium")]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("major", "minor", "patch", "promote-rc", "promote-release", "start-dev")]
    [string] $Action,

    [ValidateSet("major", "minor")]
    [string] $Bump = "minor"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -Path $repoRoot

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "cargo was not found on PATH"
}

$metadataOutput = & cargo metadata --no-deps --format-version 1
if ($LASTEXITCODE -ne 0) {
    throw "cargo metadata failed with exit code $LASTEXITCODE"
}
$metadata = ($metadataOutput -join [Environment]::NewLine) | ConvertFrom-Json

$versionPattern = '^(?<major>\d+)\.(?<minor>\d+)\.(?<patch>\d+)(?:-(?<stage>dev|rc)\.(?<increment>\d+))?$'
$coordinatedPackages = @(
    $metadata.packages | Where-Object {
        $_.name -match '^nacelle(?:-|$)' -and $_.version -ne "0.0.0"
    }
)
if ($coordinatedPackages.Count -eq 0) {
    throw "No versioned Nacelle workspace packages were found"
}

$currentVersions = @($coordinatedPackages | Select-Object -ExpandProperty version -Unique)
if ($currentVersions.Count -ne 1) {
    throw "Nacelle workspace package versions are not coordinated: $($currentVersions -join ', ')"
}

$currentVersion = $currentVersions[0]
$versionMatch = [regex]::Match($currentVersion, $versionPattern)
if (-not $versionMatch.Success) {
    throw "Unsupported Nacelle version '$currentVersion'; expected major.minor.patch with an optional -dev.N or -rc.N suffix"
}

$major = [long]$versionMatch.Groups["major"].Value
$minor = [long]$versionMatch.Groups["minor"].Value
$patch = [long]$versionMatch.Groups["patch"].Value
$stage = $versionMatch.Groups["stage"].Value
$increment = if ($stage) { [long]$versionMatch.Groups["increment"].Value } else { 0 }
if ($stage -and $patch -ne 0) {
    throw "Prerelease versions must use a zero patch component; found '$currentVersion'"
}

function Format-Version {
    param(
        [long] $Major,
        [long] $Minor,
        [long] $Patch,
        [string] $Stage = "",
        [long] $Increment = 0
    )

    $baseVersion = "$Major.$Minor.$Patch"
    if ($Stage) {
        return "$baseVersion-$Stage.$Increment"
    }
    return $baseVersion
}

function Get-BumpedBaseVersion {
    param(
        [long] $Major,
        [long] $Minor,
        [long] $Patch,
        [string] $Component
    )

    switch ($Component) {
        "major" { return [pscustomobject]@{ Major = $Major + 1; Minor = 0; Patch = 0 } }
        "minor" { return [pscustomobject]@{ Major = $Major; Minor = $Minor + 1; Patch = 0 } }
        "patch" { return [pscustomobject]@{ Major = $Major; Minor = $Minor; Patch = $Patch + 1 } }
    }
}

switch ($Action) {
    { $_ -in @("major", "minor", "patch") } {
        if ($stage) {
            if ($Action -eq "patch") {
                $increment++
            }
            else {
                $bumped = Get-BumpedBaseVersion -Major $major -Minor $minor -Patch $patch -Component $Action
                $major = $bumped.Major
                $minor = $bumped.Minor
                $patch = $bumped.Patch
                $increment = 1
            }
            $newVersion = Format-Version -Major $major -Minor $minor -Patch $patch -Stage $stage -Increment $increment
        }
        else {
            $bumped = Get-BumpedBaseVersion -Major $major -Minor $minor -Patch $patch -Component $Action
            $newVersion = Format-Version -Major $bumped.Major -Minor $bumped.Minor -Patch $bumped.Patch
        }
    }
    "promote-rc" {
        if ($stage -ne "dev") {
            throw "promote-rc requires a current -dev.N version; found '$currentVersion'"
        }
        $newVersion = Format-Version -Major $major -Minor $minor -Patch $patch -Stage "rc" -Increment $increment
    }
    "promote-release" {
        if ($stage -ne "rc") {
            throw "promote-release requires a current -rc.N version; found '$currentVersion'"
        }
        if ($patch -ne 0) {
            throw "Prerelease versions must have a zero patch component before promotion; found '$currentVersion'"
        }
        $newVersion = Format-Version -Major $major -Minor $minor -Patch $patch
    }
    "start-dev" {
        if ($stage) {
            throw "start-dev requires a release version; found '$currentVersion'"
        }
        $bumped = Get-BumpedBaseVersion -Major $major -Minor $minor -Patch $patch -Component $Bump
        $newVersion = Format-Version -Major $bumped.Major -Minor $bumped.Minor -Patch $bumped.Patch -Stage "dev" -Increment 1
    }
}

$packageNames = @{}
$packageManifestPaths = @{}
foreach ($package in $coordinatedPackages) {
    $packageNames[$package.name] = $true
    $packageManifestPaths[[IO.Path]::GetFullPath($package.manifest_path)] = $true
}

$manifestPaths = @(
    $metadata.packages |
        ForEach-Object { [IO.Path]::GetFullPath($_.manifest_path) } |
        Select-Object -Unique
    [IO.Path]::GetFullPath((Join-Path $repoRoot "Cargo.toml"))
)
$plannedFiles = @()
foreach ($manifestPath in $manifestPaths) {
    $content = [IO.File]::ReadAllText($manifestPath)
    $lineParts = [regex]::Split($content, '(?<=\n)')
    $section = ""
    $isCoordinatedPackage = $packageManifestPaths.ContainsKey($manifestPath)
    $updatedPackageVersion = $false
    $updatedContent = foreach ($line in $lineParts) {
        $sectionMatch = [regex]::Match($line, '^\s*\[(?<section>[^\]]+)\]')
        if ($sectionMatch.Success) {
            $section = $sectionMatch.Groups["section"].Value
        }

        $updatedLine = $line
        if ($isCoordinatedPackage -and $section -eq "package") {
            $packageVersionMatch = [regex]::Match($updatedLine, '^(?<prefix>\s*version\s*=\s*")(?<version>[^"]+)(?<suffix>")')
            if ($packageVersionMatch.Success) {
                if ($packageVersionMatch.Groups["version"].Value -ne $currentVersion) {
                    throw "Package version changed unexpectedly in '$manifestPath'"
                }
                $updatedLine = $packageVersionMatch.Groups["prefix"].Value + $newVersion + $packageVersionMatch.Groups["suffix"].Value + $updatedLine.Substring($packageVersionMatch.Length)
                $updatedPackageVersion = $true
            }
        }

        foreach ($packageName in $packageNames.Keys) {
            $dependencyPattern = '^(?<prefix>[ \t]*' + [regex]::Escape($packageName) + '[ \t]*=[ \t]*\{[^\r\n]*?\bversion[ \t]*=[ \t]*")(?<version>[^"]+)(?<suffix>")'
            $dependencyMatch = [regex]::Match($updatedLine, $dependencyPattern)
            if ($dependencyMatch.Success) {
                if ($dependencyMatch.Groups["version"].Value -eq $currentVersion) {
                    $updatedLine = $dependencyMatch.Groups["prefix"].Value + $newVersion + $dependencyMatch.Groups["suffix"].Value + $updatedLine.Substring($dependencyMatch.Length)
                }
            }
        }
        $updatedLine
    }

    if ($isCoordinatedPackage -and -not $updatedPackageVersion) {
        throw "Could not find the package version in '$manifestPath'"
    }

    $updatedContent = $updatedContent -join ""
    if ($updatedContent -cne $content) {
        $plannedFiles += [pscustomobject]@{
            Path = $manifestPath
            Content = $updatedContent
        }
    }
}

Write-Host "==> Nacelle crate versions: $currentVersion -> $newVersion"
Write-Host "==> Updating $($coordinatedPackages.Count) coordinated packages and $($plannedFiles.Count) Cargo.toml files"
if (-not $PSCmdlet.ShouldProcess($repoRoot, "Update crate versions and Cargo.lock to $newVersion")) {
    return
}

$utf8WithoutBom = [System.Text.UTF8Encoding]::new($false)
foreach ($file in $plannedFiles) {
    [IO.File]::WriteAllText($file.Path, $file.Content, $utf8WithoutBom)
}

& cargo update --workspace
if ($LASTEXITCODE -ne 0) {
    throw "cargo update --workspace failed with exit code $LASTEXITCODE"
}

Write-Host "==> Updated Nacelle crates to $newVersion"
