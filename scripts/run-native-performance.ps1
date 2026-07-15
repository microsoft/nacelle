<#
.SYNOPSIS
Detects Linux CPU/NUMA topology and runs native Nacelle performance workloads.

.EXAMPLE
./scripts/run-native-performance.ps1 -PlanOnly

.EXAMPLE
./scripts/run-native-performance.ps1 -Runs 3 -DurationSecs 30 -WarmupSecs 15

.EXAMPLE
./scripts/run-native-performance.ps1 -Profile min-rtt,pooled
#>
param(
    [ValidateSet("all", "capacity", "min-rtt", "pooled", "pool-saturation", "throughput")]
    [string[]] $Profile = @("capacity"),
    [int[]] $CapacityWorkerCounts = @(1, 2, 4, 8, 16, 32, 36),
    [int[]] $CapacityResponseKiB = @(0, 1, 10, 100),
    [ValidateRange(1, 100)][int] $Runs = 3,
    [ValidateRange(1, 3600)][int] $DurationSecs = 30,
    [ValidateRange(0, 600)][int] $WarmupSecs = 15,
    [string] $Bind = "127.0.0.1:7878",
    [string] $Config = "examples/nacelle-stress-server/configs/tcp.toml",
    [string] $OutputDirectory = "target/native-performance",
    [switch] $PlanOnly,
    [switch] $SkipBuild
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Assert-Command {
    param([Parameter(Mandatory)][string] $Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found on PATH."
    }
}

function Invoke-CapturedCommand {
    param(
        [Parameter(Mandatory)][string] $Command,
        [AllowEmptyCollection()][string[]] $Arguments = @()
    )

    $output = & $Command @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "$Command $($Arguments -join ' ') failed:`n$($output -join [Environment]::NewLine)"
    }
    return ($output | Out-String).Trim()
}

function Resolve-RepoPath {
    param([Parameter(Mandatory)][string] $Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $RepoRoot $Path))
}

function Get-CpuTopology {
    $rows = @()
    foreach ($line in (& lscpu "-p=CPU,CORE,SOCKET,NODE")) {
        if ($line.StartsWith("#") -or [string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        $fields = $line.Split(",")
        $node = if ($fields[3] -eq "-") { 0 } else { [int]$fields[3] }
        $rows += [pscustomobject]@{
            Cpu    = [int]$fields[0]
            Core   = [int]$fields[1]
            Socket = [int]$fields[2]
            Node   = $node
        }
    }
    if ($LASTEXITCODE -ne 0 -or $rows.Count -eq 0) {
        throw "Unable to read CPU topology from lscpu."
    }

    $physicalCores = @(
        $rows |
        Group-Object Socket, Core |
        ForEach-Object {
            $siblings = @($_.Group | Sort-Object Cpu)
            [pscustomobject]@{
                Socket   = $siblings[0].Socket
                Node     = $siblings[0].Node
                Core     = $siblings[0].Core
                Cpu      = $siblings[0].Cpu
                Siblings = @($siblings.Cpu)
            }
        } |
        Sort-Object Node, Socket, Core
    )

    return [pscustomobject]@{
        LogicalCpuCount   = $rows.Count
        PhysicalCoreCount = $physicalCores.Count
        SocketCount       = @($rows.Socket | Sort-Object -Unique).Count
        NodeCount         = @($rows.Node | Sort-Object -Unique).Count
        ThreadsPerCore    = ($physicalCores | ForEach-Object { $_.Siblings.Count } | Measure-Object -Maximum).Maximum
        Cores             = $physicalCores
    }
}

function Split-Cpus {
    param([Parameter(Mandatory)][int[]] $Cpus)

    if ($Cpus.Count -lt 2) {
        throw "At least two physical cores are required after reserving OS cores."
    }
    $serverCount = [Math]::Max(1, [Math]::Floor($Cpus.Count / 2))
    return [pscustomobject]@{
        Server = @($Cpus[0..($serverCount - 1)])
        Client = @($Cpus[$serverCount..($Cpus.Count - 1)])
    }
}

function Convert-CpuList {
    param([Parameter(Mandatory)][int[]] $Cpus)
    return ($Cpus -join ",")
}

function Get-HostPlan {
    param(
        [Parameter(Mandatory)] $Topology,
        [Parameter(Mandatory)][int[]] $WorkerCounts,
        [Parameter(Mandatory)][int[]] $ResponseSizesKiB
    )

    $nodeGroups = @($Topology.Cores | Group-Object Node | Sort-Object { [int]$_.Name })
    $availableByNode = @{}
    $reserved = @()
    foreach ($nodeGroup in $nodeGroups) {
        $cpus = @($nodeGroup.Group.Cpu)
        if ($cpus.Count -lt 3) {
            throw "NUMA node $($nodeGroup.Name) needs at least three physical cores."
        }
        $reserved += $cpus[0]
        $availableByNode[[int]$nodeGroup.Name] = @($cpus[1..($cpus.Count - 1)])
    }

    $primaryNode = [int]$nodeGroups[0].Name
    $primaryCpus = [int[]]$availableByNode[$primaryNode]
    $sameNode = Split-Cpus $primaryCpus

    if ($nodeGroups.Count -gt 1) {
        $throughputServerNode = $primaryNode
        $throughputClientNode = [int]$nodeGroups[1].Name
        $throughputServerCpus = [int[]]$availableByNode[$throughputServerNode]
        $throughputClientCpus = [int[]]$availableByNode[$throughputClientNode]
    }
    else {
        $throughputSplit = Split-Cpus $primaryCpus
        $throughputServerNode = $primaryNode
        $throughputClientNode = $primaryNode
        $throughputServerCpus = [int[]]$throughputSplit.Server
        $throughputClientCpus = [int[]]$throughputSplit.Client
    }

    $diagnosticProfiles = @(
        [pscustomobject]@{
            Name          = "min-rtt"
            Connections   = 1
            Pipeline      = 1
            PayloadBytes  = 64
            ResponseBytes = 64
            ServerCpus    = @($primaryCpus[0])
            ClientCpus    = @($primaryCpus[1])
            ServerNode    = $primaryNode
            ClientNode    = $primaryNode
            ServerThreads = 1
        },
        [pscustomobject]@{
            Name          = "pooled"
            Connections   = 32
            Pipeline      = 1
            PayloadBytes  = 256
            ResponseBytes = 64
            ServerCpus    = @($sameNode.Server)
            ClientCpus    = @($sameNode.Client)
            ServerNode    = $primaryNode
            ClientNode    = $primaryNode
            ServerThreads = $sameNode.Server.Count
        },
        [pscustomobject]@{
            Name          = "pool-saturation"
            Connections   = 256
            Pipeline      = 1
            PayloadBytes  = 256
            ResponseBytes = 64
            ServerCpus    = @($throughputServerCpus)
            ClientCpus    = @($throughputClientCpus)
            ServerNode    = $throughputServerNode
            ClientNode    = $throughputClientNode
            ServerThreads = $throughputServerCpus.Count
        },
        [pscustomobject]@{
            Name          = "throughput"
            Connections   = 256
            Pipeline      = 8
            PayloadBytes  = 256
            ResponseBytes = 64
            ServerCpus    = @($throughputServerCpus)
            ClientCpus    = @($throughputClientCpus)
            ServerNode    = $throughputServerNode
            ClientNode    = $throughputClientNode
            ServerThreads = $throughputServerCpus.Count
        }
    )

    $capacityProfiles = @()
    $omittedWorkerCounts = @()
    foreach ($workerCount in $WorkerCounts | Sort-Object -Unique) {
        if ($workerCount -lt 1) {
            throw "Capacity worker counts must be greater than zero."
        }
        if ($workerCount -gt $throughputServerCpus.Count) {
            $omittedWorkerCounts += $workerCount
            continue
        }
        $serverCpus = @($throughputServerCpus[0..($workerCount - 1)])
        foreach ($responseKiB in $ResponseSizesKiB | Sort-Object -Unique) {
            if ($responseKiB -lt 0) {
                throw "Capacity response sizes cannot be negative."
            }
            $capacityProfiles += [pscustomobject]@{
                Name          = "capacity-$($workerCount)cpu-$($responseKiB)kb"
                Connections   = 50
                Pipeline      = 1
                PayloadBytes  = 0
                ResponseBytes = $responseKiB * 1024
                ServerCpus    = $serverCpus
                ClientCpus    = @($throughputClientCpus)
                ServerNode    = $throughputServerNode
                ClientNode    = $throughputClientNode
                ServerThreads = $workerCount
            }
        }
    }

    return [pscustomobject]@{
        ReservedOsCpus       = $reserved
        AvailableServerCores = $throughputServerCpus.Count
        AvailableClientCores = $throughputClientCpus.Count
        CapacityProfiles     = $capacityProfiles
        DiagnosticProfiles   = $diagnosticProfiles
        OmittedWorkerCounts  = @($omittedWorkerCounts | Sort-Object -Unique)
        Profiles             = @($capacityProfiles) + @($diagnosticProfiles)
    }
}

function Wait-PortOpen {
    param(
        [Parameter(Mandatory)][string] $Address,
        $Process,
        [int] $TimeoutSeconds = 10
    )

    $separator = $Address.LastIndexOf(":")
    if ($separator -lt 1) {
        throw "Invalid bind address '$Address'. Expected host:port."
    }
    $hostName = $Address.Substring(0, $separator)
    $port = [int]$Address.Substring($separator + 1)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)

    while ([DateTime]::UtcNow -lt $deadline) {
        if ($null -ne $Process -and $Process.HasExited) {
            throw "Server exited with code $($Process.ExitCode) before listening on $Address."
        }
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $connect = $client.ConnectAsync($hostName, $port)
            if ($connect.Wait(100) -and $client.Connected) {
                return
            }
        }
        catch {
            # The server may still be starting.
        }
        finally {
            $client.Dispose()
        }
        Start-Sleep -Milliseconds 100
    }
    throw "Timed out waiting for $Address."
}

function Test-PortOpen {
    param([Parameter(Mandatory)][string] $Address)

    try {
        Wait-PortOpen -Address $Address -TimeoutSeconds 1
        return $true
    }
    catch {
        return $false
    }
}

function Get-TomlBool {
    param(
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][string] $Key
    )

    if (-not (Test-Path $Path)) {
        return $null
    }
    foreach ($line in Get-Content $Path) {
        $value = ($line -replace '\s*#.*$', '').Trim()
        if ($value -match "^$([regex]::Escape($Key))\s*=\s*(true|false)\s*$") {
            return $Matches[1] -eq "true"
        }
    }
    return $null
}

function Get-EffectiveTlsSelfSigned {
    param([Parameter(Mandatory)][string] $ConfigPath)

    $enabled = Get-TomlBool (Join-Path $RepoRoot "config.toml") "tls_self_signed"
    if ($null -eq $enabled) {
        $enabled = $false
    }
    if ($ConfigPath -ne (Join-Path $RepoRoot "config.toml")) {
        $override = Get-TomlBool $ConfigPath "tls_self_signed"
        if ($null -ne $override) {
            $enabled = $override
        }
    }
    return $enabled
}

function Invoke-StressClient {
    param(
        [Parameter(Mandatory)] $ProfileConfig,
        [Parameter(Mandatory)][int] $Seconds,
        [Parameter(Mandatory)][string] $ClientBinary,
        [Parameter(Mandatory)][bool] $TlsEnabled,
        [string] $LogPath
    )

    $arguments = @(
        "--physcpubind=$(Convert-CpuList $ProfileConfig.ClientCpus)",
        "--membind=$($ProfileConfig.ClientNode)",
        $ClientBinary,
        "--addr", $Bind,
        "--connections", "$($ProfileConfig.Connections)",
        "--pipeline", "$($ProfileConfig.Pipeline)",
        "--duration-secs", "$Seconds",
        "--payload-bytes", "$($ProfileConfig.PayloadBytes)"
    )
    if ($TlsEnabled) {
        $arguments += "--tls-insecure"
    }

    $outputLines = @(& numactl @arguments 2>&1)
    $exitCode = $LASTEXITCODE
    $output = ($outputLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    if ($LogPath) {
        Set-Content -Path $LogPath -Value $output
    }
    if ($exitCode -ne 0) {
        throw "Stress client failed with exit code $exitCode.`n$output"
    }
    return $output
}

function Convert-StressResult {
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $Output,
        [Parameter(Mandatory)] $ProfileConfig,
        [Parameter(Mandatory)][int] $Run
    )

    $readNumber = {
        param([string] $Name)
        if ($Output -match "(?m)^$([regex]::Escape($Name))\s+([0-9.]+)") {
            return [double]$Matches[1]
        }
        return $null
    }
    return [ordered]@{
        profile            = $ProfileConfig.Name
        run                = $Run
        completed_requests = & $readNumber "completed_requests"
        effective_rps      = & $readNumber "effective_rps"
        avg_latency_us     = & $readNumber "avg_latency"
        max_latency_us     = & $readNumber "max_latency"
        p50_latency_us     = & $readNumber "p50_latency"
        p95_latency_us     = & $readNumber "p95_latency"
        p99_latency_us     = & $readNumber "p99_latency"
    }
}

function Get-Median {
    param([AllowEmptyCollection()][object[]] $Values)

    $numbers = @($Values | Where-Object { $null -ne $_ } | Sort-Object)
    if ($numbers.Count -eq 0) {
        return $null
    }
    $middle = [Math]::Floor($numbers.Count / 2)
    if ($numbers.Count % 2 -eq 1) {
        return [double]$numbers[$middle]
    }
    return ([double]$numbers[$middle - 1] + [double]$numbers[$middle]) / 2
}

function Stop-Server {
    param($Process)

    if ($null -eq $Process -or $Process.HasExited) {
        return
    }
    Stop-Process -Id $Process.Id
    if (-not $Process.WaitForExit(5000)) {
        Stop-Process -Id $Process.Id -Force
        $Process.WaitForExit()
    }
}

if (-not $IsLinux) {
    throw "This harness requires native Linux."
}
Assert-Command lscpu

$topology = Get-CpuTopology
$hostPlan = Get-HostPlan `
    -Topology $topology `
    -WorkerCounts $CapacityWorkerCounts `
    -ResponseSizesKiB $CapacityResponseKiB
$selectedProfiles = if ($Profile -contains "all") {
    @($hostPlan.Profiles)
}
else {
    @(
        if ($Profile -contains "capacity") {
            $hostPlan.CapacityProfiles
        }
        $hostPlan.DiagnosticProfiles | Where-Object { $Profile -contains $_.Name }
    )
}
if ($selectedProfiles.Count -eq 0) {
    throw "The selected profile and worker counts produced no runnable workloads on this host."
}

$configPath = Resolve-RepoPath $Config
$outputRoot = Resolve-RepoPath $OutputDirectory
$timestamp = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
$runDirectory = Join-Path $outputRoot $timestamp
[System.IO.Directory]::CreateDirectory($runDirectory) | Out-Null

$governors = @(
    Get-ChildItem "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor" -ErrorAction SilentlyContinue |
    ForEach-Object { Get-Content $_.FullName } |
    Sort-Object -Unique
)
$memory = Get-Content "/proc/meminfo" | Where-Object { $_ -match '^(MemTotal|MemAvailable|SwapTotal|SwapFree):' }
$virtualization = "unknown"
if (Get-Command systemd-detect-virt -ErrorAction SilentlyContinue) {
    $detectedVirtualization = & systemd-detect-virt 2>$null
    $virtualization = if ($LASTEXITCODE -eq 0) { ($detectedVirtualization | Out-String).Trim() } else { "none" }
}
$hostMetadata = [ordered]@{
    captured_at_utc  = [DateTime]::UtcNow.ToString("o")
    hostname         = [System.Net.Dns]::GetHostName()
    kernel           = Invoke-CapturedCommand uname @("-a")
    commit           = Invoke-CapturedCommand git @("-C", $RepoRoot, "rev-parse", "HEAD")
    dirty            = -not [string]::IsNullOrWhiteSpace((Invoke-CapturedCommand git @("-C", $RepoRoot, "status", "--porcelain")))
    logical_cpus     = $topology.LogicalCpuCount
    physical_cores   = $topology.PhysicalCoreCount
    sockets          = $topology.SocketCount
    numa_nodes       = $topology.NodeCount
    threads_per_core = $topology.ThreadsPerCore
    reserved_os_cpus = @($hostPlan.ReservedOsCpus)
    governors        = $governors
    virtualization   = $virtualization
    memory           = $memory
    lscpu            = Invoke-CapturedCommand lscpu
    numactl          = if (Get-Command numactl -ErrorAction SilentlyContinue) {
        Invoke-CapturedCommand numactl @("--hardware")
    }
    else { $null }
}
$plan = [ordered]@{
    host             = $hostMetadata
    bind             = $Bind
    config           = $configPath
    runs             = $Runs
    warmup_seconds   = $WarmupSecs
    duration_seconds = $DurationSecs
    profiles         = @($selectedProfiles)
}
$planPath = Join-Path $runDirectory "plan.json"
$plan | ConvertTo-Json -Depth 8 | Set-Content $planPath

Write-Host "==> Detected $($topology.PhysicalCoreCount) physical cores, $($topology.LogicalCpuCount) logical CPUs, $($topology.SocketCount) sockets, $($topology.NodeCount) NUMA nodes"
Write-Host "==> Reserved OS CPUs: $(Convert-CpuList $hostPlan.ReservedOsCpus)"
if ($hostPlan.OmittedWorkerCounts.Count -gt 0) {
    Write-Warning "Omitting capacity worker counts [$($hostPlan.OmittedWorkerCounts -join ',')] because only $($hostPlan.AvailableClientCores) isolated client cores and $($hostPlan.AvailableServerCores) server cores are available to this single-host plan."
}
if ($virtualization -ne "none" -and $virtualization -ne "unknown") {
    Write-Warning "Virtualization detected: $virtualization. Treat measurements as virtual-host results."
}
if ($governors.Count -gt 0 -and $governors -notcontains "performance") {
    Write-Warning "CPU governor is '$($governors -join ',')', not 'performance'."
}
foreach ($profileConfig in $selectedProfiles) {
    Write-Host "==> $($profileConfig.Name): server=[$(Convert-CpuList $profileConfig.ServerCpus)] node=$($profileConfig.ServerNode) client=[$(Convert-CpuList $profileConfig.ClientCpus)] node=$($profileConfig.ClientNode) threads=$($profileConfig.ServerThreads) connections=$($profileConfig.Connections) pipeline=$($profileConfig.Pipeline) request=$($profileConfig.PayloadBytes)B response=$($profileConfig.ResponseBytes)B"
}
Write-Host "==> Plan: $planPath"

if ($PlanOnly) {
    return
}

Assert-Command numactl
Assert-Command cargo
if (-not (Test-Path $configPath)) {
    throw "Configuration file was not found: $configPath"
}
if (Test-PortOpen $Bind) {
    throw "A process is already listening on $Bind."
}
$bindingProbe = $selectedProfiles[0]
& numactl `
    "--physcpubind=$(Convert-CpuList $bindingProbe.ServerCpus)" `
    "--membind=$($bindingProbe.ServerNode)" `
    true
if ($LASTEXITCODE -ne 0) {
    throw "The detected CPU and NUMA memory binding is not permitted on this host."
}

Set-Location $RepoRoot
if (-not $SkipBuild) {
    Write-Host "==> Building native release stress binaries"
    & cargo build --release -p nacelle-stress-server -p nacelle-stress-test --features nacelle-stress-server/exp-memory-limits
    if ($LASTEXITCODE -ne 0) {
        throw "Release build failed with exit code $LASTEXITCODE."
    }
}

$serverBinary = Join-Path $RepoRoot "target/release/nacelle-stress-server"
$clientBinary = Join-Path $RepoRoot "target/release/nacelle-stress-test"
if (-not (Test-Path $serverBinary) -or -not (Test-Path $clientBinary)) {
    throw "Release stress binaries are missing. Remove -SkipBuild or build them first."
}

$tlsEnabled = Get-EffectiveTlsSelfSigned $configPath
$results = @()
foreach ($profileConfig in $selectedProfiles) {
    for ($run = 1; $run -le $Runs; $run++) {
        $prefix = "$($profileConfig.Name)-run-$run"
        $serverStdout = Join-Path $runDirectory "$prefix-server.stdout.log"
        $serverStderr = Join-Path $runDirectory "$prefix-server.stderr.log"
        $clientLog = Join-Path $runDirectory "$prefix-client.log"
        $serverArguments = @(
            "--physcpubind=$(Convert-CpuList $profileConfig.ServerCpus)",
            "--membind=$($profileConfig.ServerNode)",
            $serverBinary,
            "--bind", $Bind,
            "--server-threads", "$($profileConfig.ServerThreads)",
            "--response-bytes", "$($profileConfig.ResponseBytes)"
        )
        if ($configPath -ne (Join-Path $RepoRoot "config.toml")) {
            $serverArguments += @("--config", $configPath)
        }

        $server = $null
        try {
            Write-Host "==> Running $($profileConfig.Name) sample $run/$Runs (request=$($profileConfig.PayloadBytes)B response=$($profileConfig.ResponseBytes)B)"
            $server = Start-Process `
                -FilePath (Get-Command numactl).Source `
                -ArgumentList $serverArguments `
                -WorkingDirectory $RepoRoot `
                -RedirectStandardOutput $serverStdout `
                -RedirectStandardError $serverStderr `
                -PassThru
            Wait-PortOpen -Address $Bind -Process $server

            if ($WarmupSecs -gt 0) {
                $null = Invoke-StressClient `
                    -ProfileConfig $profileConfig `
                    -Seconds $WarmupSecs `
                    -ClientBinary $clientBinary `
                    -TlsEnabled $tlsEnabled
            }

            $clientOutput = Invoke-StressClient `
                -ProfileConfig $profileConfig `
                -Seconds $DurationSecs `
                -ClientBinary $clientBinary `
                -TlsEnabled $tlsEnabled `
                -LogPath $clientLog
            $clientOutput | Out-Host
            $result = Convert-StressResult $clientOutput $profileConfig $run
            $results += [pscustomobject]$result
            $result | ConvertTo-Json | Set-Content (Join-Path $runDirectory "$prefix-result.json")
        }
        finally {
            Stop-Server $server
        }
    }
}

$summaryPath = Join-Path $runDirectory "results.json"
$medians = @(
    $results |
    Group-Object profile |
    ForEach-Object {
        $samples = @($_.Group)
        [ordered]@{
            profile        = $_.Name
            runs           = $samples.Count
            effective_rps  = Get-Median @($samples.effective_rps)
            avg_latency_us = Get-Median @($samples.avg_latency_us)
            max_latency_us = Get-Median @($samples.max_latency_us)
            p50_latency_us = Get-Median @($samples.p50_latency_us)
            p95_latency_us = Get-Median @($samples.p95_latency_us)
            p99_latency_us = Get-Median @($samples.p99_latency_us)
        }
    }
)
[ordered]@{
    medians = $medians
    runs    = $results
} | ConvertTo-Json -Depth 6 | Set-Content $summaryPath
Write-Host "==> Results: $summaryPath"