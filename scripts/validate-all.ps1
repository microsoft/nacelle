$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [string] $Name,
        [scriptblock] $Command
    )

    Write-Host "==> $Name"
    & $Command
    $commandSucceeded = $?
    $exitCode = $LASTEXITCODE
    if (-not $commandSucceeded -or $exitCode -ne 0) {
        throw "$Name failed with exit code $exitCode"
    }
}

Invoke-Step "cargo fmt" { cargo fmt --all -- --check }
Invoke-Step "workspace tests" { cargo test --workspace --all-targets }
Invoke-Step "workspace clippy" { cargo clippy --workspace --all-targets -- -D warnings }
Invoke-Step "nacelle-core full tests" { cargo test -p nacelle-core --features "tls" --all-targets }
Invoke-Step "nacelle-core full clippy" { cargo clippy -p nacelle-core --features "tls" --all-targets -- -D warnings }
Invoke-Step "nacelle-rustls full tests" { cargo test -p nacelle-rustls --all-features --all-targets }
Invoke-Step "nacelle-rustls full clippy" { cargo clippy -p nacelle-rustls --all-features --all-targets -- -D warnings }
Invoke-Step "nacelle-openssl tests" { cargo test -p nacelle-openssl --all-targets }
Invoke-Step "nacelle-openssl clippy" { cargo clippy -p nacelle-openssl --all-targets -- -D warnings }
Invoke-Step "nacelle-tcp tests" { cargo test -p nacelle-tcp --all-targets }
Invoke-Step "nacelle-tcp clippy" { cargo clippy -p nacelle-tcp --all-targets -- -D warnings }
Invoke-Step "nacelle-tcp tls tests" { cargo test -p nacelle-tcp --features tls-self-signed --all-targets }
Invoke-Step "nacelle-tcp tls clippy" { cargo clippy -p nacelle-tcp --features tls-self-signed --all-targets -- -D warnings }
Invoke-Step "nacelle-http full tests" { cargo test -p nacelle-http --features tls-self-signed --all-targets }
Invoke-Step "nacelle-http full clippy" { cargo clippy -p nacelle-http --features tls-self-signed --all-targets -- -D warnings }
Invoke-Step "reference protocol tests" { cargo test -p nacelle-reference-protocol --all-targets }
Invoke-Step "reference protocol clippy" { cargo clippy -p nacelle-reference-protocol --all-targets -- -D warnings }
Invoke-Step "examples check" { cargo check -p nacelle-examples --all-features --all-targets }
Invoke-Step "examples clippy" { cargo clippy -p nacelle-examples --all-features --all-targets -- -D warnings }
Invoke-Step "nacelle full tests" { cargo test -p nacelle --features "http" --all-targets }
Invoke-Step "nacelle full clippy" { cargo clippy -p nacelle --features "http" --all-targets -- -D warnings }
Invoke-Step "nacelle http tests" { cargo test -p nacelle --no-default-features --features http --all-targets }
Invoke-Step "nacelle tls tests" { cargo test -p nacelle --no-default-features --features tls --all-targets }
Invoke-Step "nacelle tcp-only clippy" { cargo clippy -p nacelle --no-default-features --features tcp --all-targets -- -D warnings }
Invoke-Step "nacelle self-signed tls tests" { cargo test -p nacelle --no-default-features --features tls-self-signed --all-targets }
Invoke-Step "nacelle https self-signed tests" { cargo test -p nacelle --no-default-features --features "http,tls-self-signed" --all-targets }
Invoke-Step "nacelle TCP self-signed tests" { cargo test -p nacelle --features tls-self-signed --all-targets }
Invoke-Step "nacelle all-feature clippy" { cargo clippy -p nacelle --features "http,tls-self-signed" --all-targets -- -D warnings }
Invoke-Step "nacelle no-default tests" { cargo test -p nacelle --no-default-features --all-targets }
Invoke-Step "stress client tests" { cargo test -p nacelle-stress-test --all-targets }
Invoke-Step "stress client no-default tests" { cargo test -p nacelle-stress-test --no-default-features --all-targets }
Invoke-Step "stress server tests" { cargo test -p nacelle-stress-server --all-targets }
Invoke-Step "stress server no-default tests" { cargo test -p nacelle-stress-server --no-default-features --all-targets }

cargo tree -i serde_yaml *> $null
if ($LASTEXITCODE -eq 0) {
    throw "serde_yaml is still present"
}

cargo tree -i unsafe-libyaml *> $null
if ($LASTEXITCODE -eq 0) {
    throw "unsafe-libyaml is still present"
}

cargo tree -p nacelle --no-default-features --features "tcp,openssl" -i rustls *> $null
if ($LASTEXITCODE -eq 0) {
    throw "rustls is selected by the tcp,openssl feature set"
}

cargo tree -p nacelle --no-default-features -i rustls *> $null
if ($LASTEXITCODE -eq 0) {
    throw "rustls is selected by the nacelle no-default feature set"
}

if (Get-Command cargo-audit -ErrorAction SilentlyContinue) {
    Invoke-Step "cargo audit" { cargo audit }
}
else {
    Write-Warning "cargo-audit not installed; skipping audit"
}
