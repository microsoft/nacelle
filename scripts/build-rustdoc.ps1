param(
    [switch]$Open
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -Path $repoRoot

Write-Host "==> Building Rust API documentation"
$previousRustdocFlags = $env:RUSTDOCFLAGS
$env:RUSTDOCFLAGS = (($previousRustdocFlags, "-D warnings") | Where-Object { $_ } | Select-Object -Unique) -join " "

try {
    cargo doc -p nacelle --no-default-features --features "buffer-rotation,error-hints,experimental-memory,experimental-thread-per-core,http,phase-timing,rustls,tcp,tls-self-signed" --no-deps
    if ($LASTEXITCODE -ne 0) {
        throw "Rustls facade documentation failed with exit code $LASTEXITCODE"
    }
    cargo doc -p nacelle-openssl --all-features --no-deps
    if ($LASTEXITCODE -ne 0) {
        throw "OpenSSL provider documentation failed with exit code $LASTEXITCODE"
    }
} finally {
    $env:RUSTDOCFLAGS = $previousRustdocFlags
}

$indexPath = Join-Path $repoRoot "target\doc\nacelle\index.html"
Write-Host "==> API docs: $indexPath"

if ($Open) {
    Invoke-Item -Path $indexPath
}
