param(
    [string]$Bind = "127.0.0.1:7878",
    [int]$ServerThreads = [Environment]::ProcessorCount,
    [int]$DurationSecs = 30,
    [int]$Connections = 256,
    [int]$Pipeline = 8,
    [int]$PayloadBytes = 256,
    [string]$Config = "config.toml",
    [switch]$Debug
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location -Path $RepoRoot

function Invoke-Cargo {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )

    & cargo @Args
    if ($LASTEXITCODE -ne 0) {
        throw "cargo $($Args -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Wait-PortOpen {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Bind,
        [int]$TimeoutMs = 5000
    )

    $idx = $Bind.LastIndexOf(':')
    if ($idx -lt 1) {
        throw "Invalid --Bind '$Bind'. Expected host:port"
    }

    $hostName = $Bind.Substring(0, $idx)
    $port = [int]$Bind.Substring($idx + 1)
    $deadline = [DateTime]::UtcNow.AddMilliseconds($TimeoutMs)

    while ([DateTime]::UtcNow -lt $deadline) {
        $client = New-Object System.Net.Sockets.TcpClient
        try {
            $task = $client.ConnectAsync($hostName, $port)
            if ($task.Wait(100) -and $client.Connected) {
                return
            }
        } catch {
            # Ignore while waiting for the server to begin listening.
        } finally {
            $client.Dispose()
        }

        Start-Sleep -Milliseconds 100
    }

    throw "Timed out waiting for $Bind to accept connections"
}

function Get-TomlBool {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Key
    )

    $configPath = if ([System.IO.Path]::IsPathRooted($Path)) {
        $Path
    } else {
        Join-Path $RepoRoot $Path
    }

    if (-not (Test-Path -Path $configPath)) {
        return $null
    }

    foreach ($line in Get-Content -Path $configPath) {
        $withoutComment = ($line -replace '\s*#.*$', '').Trim()
        if ($withoutComment -match "^$([regex]::Escape($Key))\s*=\s*(true|false)\s*$") {
            return $Matches[1] -eq "true"
        }
    }

    return $null
}

function Get-EffectiveTlsSelfSigned {
    $value = Get-TomlBool -Path "config.toml" -Key "tls_self_signed"
    if ($null -eq $value) {
        $value = $false
    }

    if ($Config -ne "config.toml" -and $Config -ne ".\config.toml" -and $Config -ne "./config.toml") {
        $override = Get-TomlBool -Path $Config -Key "tls_self_signed"
        if ($null -ne $override) {
            $value = $override
        }
    }

    return $value
}

$profile = if ($Debug) { "debug" } else { "release" }
$cargoProfileArg = if ($Debug) { @() } else { @("--release") }

Write-Host "==> Building nacelle-stress-test ($profile)"
Invoke-Cargo -Args (@("build") + $cargoProfileArg + @("--package", "nacelle-stress-test"))

Write-Host "==> Building nacelle-stress-server ($profile)"
Invoke-Cargo -Args (@(
    "build"
) + $cargoProfileArg + @(
    "--package", "nacelle-stress-server"
))

$serverExe = Join-Path $RepoRoot "target\$profile\nacelle-stress-server.exe"
$clientExe = Join-Path $RepoRoot "target\$profile\nacelle-stress-test.exe"

if (-not (Test-Path -Path $serverExe)) {
    throw "Missing server binary: $serverExe"
}
if (-not (Test-Path -Path $clientExe)) {
    throw "Missing stress client binary: $clientExe"
}

$serverArgs = @(
    "--bind", $Bind,
    "--server-threads", "$ServerThreads"
)
if ($Config -ne "config.toml" -and $Config -ne ".\config.toml" -and $Config -ne "./config.toml") {
    $serverArgs += @("--config", $Config)
}

$server = $null
try {
    Write-Host "==> Starting server: $serverExe $($serverArgs -join ' ')"
    $server = Start-Process -FilePath $serverExe -ArgumentList $serverArgs -PassThru

    Wait-PortOpen -Bind $Bind -TimeoutMs 5000

    Write-Host "--- nacelle threads=$ServerThreads connections=$Connections pipeline=$Pipeline ---"

    $clientArgs = @(
        "--addr", $Bind,
        "--connections", "$Connections",
        "--pipeline", "$Pipeline",
        "--duration-secs", "$DurationSecs",
        "--payload-bytes", "$PayloadBytes"
    )
    if (Get-EffectiveTlsSelfSigned) {
        $clientArgs += "--tls-insecure"
    }
    & $clientExe @clientArgs

    if ($LASTEXITCODE -ne 0) {
        throw "nacelle-stress-test exited with code $LASTEXITCODE"
    }
}
finally {
    if ($null -ne $server -and -not $server.HasExited) {
        Write-Host "==> Stopping server (PID $($server.Id))"
        Stop-Process -Id $server.Id -Force
    }
}
