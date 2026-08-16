<#
.SYNOPSIS
    Dieu khien stack LZD Uplift tren Windows.

.DESCRIPTION
    Khong bat buoc dung script nay. `docker compose up -d` la du — moi bien
    trong compose deu co mac dinh, khong can .env, khong can build tay.
    Script chi them: preflight TLS/registry, retry pull tuan tu, va cac lenh
    tien ich (health, reconstruction, snapshot).

.EXAMPLE
    .\scripts\stack.ps1 up          # = docker compose up -d (ca nen tang)
    .\scripts\stack.ps1 up-all      # them event-producer + stream-consumer
    .\scripts\stack.ps1 up-core     # chi ha tang + airflow + api, bo observability
    .\scripts\stack.ps1 doctor      # chan doan Docker registry/proxy/CA
    .\scripts\stack.ps1 init        # tuy chon: tao .env de ghi de + build truoc
    .\scripts\stack.ps1 status      # trang thai container + link UI
    .\scripts\stack.ps1 logs airflow-scheduler
    .\scripts\stack.ps1 health      # goi thu tung endpoint
    .\scripts\stack.ps1 redis       # mo redis-cli
    .\scripts\stack.ps1 reconstruction H2  # chay Track A -> Gate -> T0 -> Track B
    .\scripts\stack.ps1 track-a 1000 # gen Track A raw events tu full_trainset.csv
    .\scripts\stack.ps1 snapshot    # ghi committed reconstruction snapshot vao docs/
    .\scripts\stack.ps1 down        # dung (giu du lieu)
    .\scripts\stack.ps1 reset       # dung + XOA het volume
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("init", "doctor", "build", "up-core", "up", "up-all", "down", "reset", "status",
                 "logs", "health", "redis", "duckdb", "reconstruction", "track-a", "snapshot", "test", "ps")]
    [string]$Command = "status",

    [Parameter(Position = 1)]
    [string]$Target = ""
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

# Stack toi thieu cho ai khong muon bat 10 container observability.
$CoreServices = @(
    "postgres", "redis", "minio", "minio-init", "kafka", "kafka-init",
    "airflow-init", "airflow-webserver", "airflow-scheduler",
    "mlflow", "inference-api"
)

function Write-Section($text) {
    Write-Host ""
    Write-Host "=== $text ===" -ForegroundColor Cyan
}

function Write-DockerFailureHelp {
    Write-Host ""
    Write-Host "Docker command failed." -ForegroundColor Red
    Write-Host "Doc dong loi THAT SU o tren truoc khi ket luan:" -ForegroundColor Yellow
    Write-Host "  - 'x509: certificate signed by unknown authority' -> loi trust/CA cua" -ForegroundColor Yellow
    Write-Host "    Docker Desktop khi pull tu Docker Hub. Sua proxy/registry CA." -ForegroundColor Yellow
    Write-Host "  - 'short read: expected N bytes but got M' / 'unexpected EOF' /" -ForegroundColor Yellow
    Write-Host "    'TLS handshake timeout' -> mang dut giua chung, KHONG phai loi CA." -ForegroundColor Yellow
    Write-Host "    Chi can chay lai; layer da tai xong deu duoc cache." -ForegroundColor Yellow
    Write-Host "Trong luc do van co the chay local: .\scripts\stack.ps1 test hoac .\scripts\stack.ps1 snapshot" -ForegroundColor Yellow
}

function Invoke-DockerPullWithRetry {
    param(
        [Parameter(Mandatory = $true)][string[]]$Images,
        [int]$MaxTries = 4
    )

    # Keo tung image mot chu khong de `docker compose up` keo song song ca chuc
    # image: tren duong truyen cham/chap chon, N stream dong thoi lam dut tai va
    # compose bao 'short read ... unexpected EOF' roi bo het.
    $failed = @()
    foreach ($img in $Images) {
        $ok = $false
        for ($try = 1; $try -le $MaxTries; $try++) {
            Write-Host "  pull [$try/$MaxTries] $img" -ForegroundColor Cyan
            & docker pull $img
            if ($LASTEXITCODE -eq 0) { $ok = $true; break }

            # Phan biet "mang chap chon" voi "TLS bi chan giua duong". Retry chi
            # cuu duoc cai dau; cai sau co thu 100 lan cung the. Probe lai bang
            # manifest inspect (re) de doc duoc thong bao loi that su.
            $probe = Invoke-CaptureNative "docker" @("manifest", "inspect", $img)
            if ($probe.ExitCode -ne 0 -and $probe.Output -match "x509|certificate signed by unknown authority|failed to verify certificate") {
                Write-Host ""
                Write-Host "  Registry tra ve certificate KHONG tin duoc - dung retry." -ForegroundColor Red
                Write-Host $probe.Output -ForegroundColor DarkYellow
                Write-DockerCaFixHelp
                throw "TLS interception / CA khong tin duoc. Xem huong dan phia tren."
            }
            Start-Sleep -Seconds 5
        }
        if (-not $ok) { $failed += $img }
    }

    if ($failed.Count -gt 0) {
        Write-Host ""
        Write-Host "Khong keo duoc cac image sau sau $MaxTries lan thu:" -ForegroundColor Red
        $failed | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
        throw "Thieu image, chua the up stack."
    }
}

function Write-DockerCaFixHelp {
    Write-Host ""
    Write-Host "Cach sua tren Docker Desktop/Windows:" -ForegroundColor Cyan
    Write-Host "  1. Docker Desktop -> Settings -> Resources -> Proxies." -ForegroundColor Yellow
    Write-Host "  2. Neu khong can proxy: dat Docker Desktop proxy va Containers proxy = No proxy." -ForegroundColor Yellow
    Write-Host "  3. Neu dung proxy cong ty/MITM: import root CA cua proxy vao Windows" -ForegroundColor Yellow
    Write-Host "     Certificates (Local Computer) -> Trusted Root Certification Authorities." -ForegroundColor Yellow
    Write-Host "  4. Restart Docker Desktop." -ForegroundColor Yellow
    Write-Host "  5. Verify: docker pull redis:7.2-alpine" -ForegroundColor Yellow
}

function Invoke-NativeChecked {
    param(
        [Parameter(Mandatory = $true)]
        [string]$File,

        [Parameter(Mandatory = $true)]
        [string[]]$Args,

        [switch]$DockerHint
    )

    & $File @Args
    $exitCode = $LASTEXITCODE
    if ($null -eq $exitCode) { $exitCode = 0 }
    if ($exitCode -ne 0) {
        if ($DockerHint) { Write-DockerFailureHelp }
        throw "Lenh that bai voi exit code ${exitCode}: $File $($Args -join ' ')"
    }
}

function Invoke-CaptureNative {
    param(
        [Parameter(Mandatory = $true)]
        [string]$File,

        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = & $File @Args 2>&1
        $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }
        return @{
            ExitCode = $exitCode
            Output = ($out | ForEach-Object { $_.ToString() }) -join "`n"
        }
    } finally {
        $ErrorActionPreference = $oldPreference
    }
}

function Test-DockerRegistryAccess {
    Write-Section "Kiem tra Docker Hub TLS"
    $probe = Invoke-CaptureNative "docker" @("manifest", "inspect", "redis:7.2-alpine")
    if ($probe.ExitCode -eq 0) {
        Write-Host "  OK  Docker co the doc Docker Hub manifest." -ForegroundColor Green
        return
    }

    Write-Host "  LOI Docker khong doc duoc Docker Hub manifest." -ForegroundColor Red
    if ($probe.Output) { Write-Host $probe.Output -ForegroundColor DarkYellow }
    if ($probe.Output -match "x509|certificate|unknown authority|tls") {
        Write-DockerCaFixHelp
    }
    throw "Docker registry preflight failed. Chua the build/up stack cho toi khi docker pull chay duoc."
}

function Get-ComposeImages {
    param([Parameter(Mandatory = $true)][string[]]$Profiles)
    $args = @("compose")
    foreach ($p in $Profiles) { $args += @("--profile", $p) }
    $args += @("config", "--images")
    $result = Invoke-CaptureNative "docker" $args
    if ($result.ExitCode -ne 0) {
        Write-Host $result.Output -ForegroundColor DarkYellow
        throw "Khong doc duoc docker compose image list."
    }
    return @($result.Output -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
}

function Test-ImagesOrRegistry {
    param([Parameter(Mandatory = $true)][string[]]$Profiles)
    $images = Get-ComposeImages $Profiles
    $missing = @()
    foreach ($img in $images) {
        $inspect = Invoke-CaptureNative "docker" @("image", "inspect", $img)
        if ($inspect.ExitCode -ne 0) { $missing += $img }
    }
    if ($missing.Count -eq 0) {
        Write-Host "  OK  Tat ca image can thiet da co local." -ForegroundColor Green
        return
    }

    Write-Section "Image con thieu"
    $missing | ForEach-Object { Write-Host "  MISSING  $_" -ForegroundColor Yellow }
    Test-DockerRegistryAccess

    Write-Section "Keo image con thieu (tuan tu, co retry)"
    Invoke-DockerPullWithRetry -Images $missing
}

function Show-Urls {
    Write-Section "Giao dien"
    @(
        @{ Name = "Airflow      "; Url = "http://localhost:8080"; Note = "admin/admin" },
        @{ Name = "Grafana      "; Url = "http://localhost:3000"; Note = "admin/admin" },
        @{ Name = "Kafka UI     "; Url = "http://localhost:8082"; Note = "topic, message, consumer lag" },
        @{ Name = "MinIO Console"; Url = "http://localhost:9001"; Note = "minioadmin/minioadmin123" },
        @{ Name = "MLflow       "; Url = "http://localhost:5000"; Note = "experiment + model registry" },
        @{ Name = "Prometheus   "; Url = "http://localhost:9090"; Note = "metric + alert" },
        @{ Name = "RedisInsight "; Url = "http://localhost:5540"; Note = "them ket noi redis:6379" },
        @{ Name = "Inference API"; Url = "http://localhost:8000/docs"; Note = "swagger" }
    ) | ForEach-Object {
        Write-Host ("  {0} {1,-32} {2}" -f $_.Name, $_.Url, $_.Note)
    }
}

switch ($Command) {

    "doctor" {
        Write-Section "Docker info"
        Invoke-NativeChecked "docker" @("version") -DockerHint
        $info = Invoke-CaptureNative "docker" @("info")
        if ($info.Output -match "HTTP Proxy|HTTPS Proxy|No Proxy") {
            ($info.Output -split "`n" | Where-Object { $_ -match "HTTP Proxy|HTTPS Proxy|No Proxy|Total Memory" }) |
                ForEach-Object { Write-Host "  $_" }
        }
        Test-DockerRegistryAccess
    }

    "init" {
        Write-Section "Tao .env (tuy chon - chi de ghi de mac dinh)"
        if (-not (Test-Path ".env")) {
            Copy-Item ".env.example" ".env"
            Write-Host "  da tao .env tu .env.example" -ForegroundColor Green
        } else {
            Write-Host "  .env da co - giu nguyen" -ForegroundColor Yellow
        }

        Write-Section "Tao thu muc runtime"
        foreach ($dir in @("airflow/logs", "airflow/plugins", "data")) {
            if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Force $dir | Out-Null }
        }

        Write-Section "Kiem tra dataset"
        foreach ($f in @("data/full_trainset.csv", "data/full_testset.csv")) {
            if (Test-Path $f) {
                $size = [math]::Round((Get-Item $f).Length / 1MB, 1)
                Write-Host "  OK  $f ($size MB)" -ForegroundColor Green
            } else {
                Write-Host "  THIEU  $f" -ForegroundColor Red
            }
        }

        Write-Section "Build image (mat vai phut lan dau)"
        Test-DockerRegistryAccess
        Invoke-NativeChecked "docker" @("compose", "--profile", "all", "build") -DockerHint
        Show-Urls
        Write-Host "`nTiep theo: .\scripts\stack.ps1 up" -ForegroundColor Cyan
    }

    "build" {
        Test-DockerRegistryAccess
        Invoke-NativeChecked "docker" @("compose", "--profile", "all", "build") -DockerHint
    }

    "up-core" {
        # Compose khong co "profile tru di": muon bo observability thi phai
        # goi ten service. Danh sach nay = stack mac dinh tru prometheus,
        # grafana, loki, promtail, exporter va cac UI.
        Test-ImagesOrRegistry @()
        Invoke-NativeChecked "docker" (@("compose", "up", "-d") + $CoreServices) -DockerHint
        Show-Urls
    }

    "up" {
        Test-ImagesOrRegistry @()
        Invoke-NativeChecked "docker" @("compose", "up", "-d") -DockerHint
        Show-Urls
    }

    "up-all" {
        Test-ImagesOrRegistry @("all")
        Invoke-NativeChecked "docker" @("compose", "--profile", "all", "up", "-d") -DockerHint
        Show-Urls
    }

    "down"  { Invoke-NativeChecked "docker" @("compose", "--profile", "all", "down") -DockerHint }

    "reset" {
        Write-Host "XOA TOAN BO du lieu (postgres, redis, minio, kafka, grafana)." -ForegroundColor Red
        $ans = Read-Host "Go 'yes' de xac nhan"
        if ($ans -eq "yes") {
            Invoke-NativeChecked "docker" @("compose", "--profile", "all", "down", "-v") -DockerHint
            Remove-Item -Recurse -Force "airflow/logs/*" -ErrorAction SilentlyContinue
            Write-Host "Da reset." -ForegroundColor Green
        } else {
            Write-Host "Huy." -ForegroundColor Yellow
        }
    }

    "ps"     { Invoke-NativeChecked "docker" @("compose", "--profile", "all", "ps") -DockerHint }

    "status" {
        Write-Section "Container"
        Invoke-NativeChecked "docker" @("compose", "--profile", "all", "ps", "--format", "table {{.Name}}\t{{.Service}}\t{{.Status}}") -DockerHint
        Show-Urls
    }

    "logs" {
        if ([string]::IsNullOrEmpty($Target)) {
            Invoke-NativeChecked "docker" @("compose", "--profile", "all", "logs", "-f", "--tail=100") -DockerHint
        } else {
            Invoke-NativeChecked "docker" @("compose", "logs", "-f", "--tail=200", $Target) -DockerHint
        }
    }

    "health" {
        Write-Section "Kiem tra suc khoe"
        $checks = @(
            @{ Name = "Airflow      "; Url = "http://localhost:8080/health" },
            @{ Name = "Grafana      "; Url = "http://localhost:3000/api/health" },
            @{ Name = "Prometheus   "; Url = "http://localhost:9090/-/healthy" },
            @{ Name = "Kafka UI     "; Url = "http://localhost:8082/actuator/health" },
            @{ Name = "MinIO        "; Url = "http://localhost:9000/minio/health/live" },
            @{ Name = "MLflow       "; Url = "http://localhost:5000/health" },
            @{ Name = "Loki         "; Url = "http://localhost:3100/ready" },
            @{ Name = "Inference API"; Url = "http://localhost:8000/health" }
        )
        foreach ($c in $checks) {
            try {
                $r = Invoke-WebRequest -Uri $c.Url -TimeoutSec 5 -UseBasicParsing
                Write-Host ("  OK   {0} ({1})" -f $c.Name, $r.StatusCode) -ForegroundColor Green
            } catch {
                Write-Host ("  LOI  {0} {1}" -f $c.Name, $_.Exception.Message) -ForegroundColor Red
            }
        }

        Write-Section "Trang thai feature store"
        try {
            $info = Invoke-RestMethod -Uri "http://localhost:8000/store/info" -TimeoutSec 5
            $info | ConvertTo-Json -Depth 4
        } catch {
            Write-Host "  Chua doc duoc /store/info (API chua chay hoac chua sync feature)" -ForegroundColor Yellow
        }
    }

    "redis"  { Invoke-NativeChecked "docker" @("compose", "exec", "redis", "redis-cli") -DockerHint }

    "duckdb" {
        $code = @"
from lzd_pipeline.common.clients import duckdb_conn
with duckdb_conn(read_only=True) as con:
    print(con.execute('SHOW ALL TABLES').fetchdf())
"@
        Invoke-NativeChecked "docker" @("compose", "exec", "airflow-scheduler", "python", "-c", $code) -DockerHint
    }

    "reconstruction" {
        Write-Section "Reconstruction E2E dry-run trong Docker"
        $branchArgs = @()
        if (-not [string]::IsNullOrEmpty($Target)) {
            if ($Target -notin @("H1", "H2")) {
                throw "Target phai la H1 hoac H2. Vi du: .\scripts\stack.ps1 reconstruction H2"
            }
            $branchArgs = @("--branch", $Target)
        }
        $schedulerId = ""
        try {
            $schedulerId = (docker compose ps -q airflow-scheduler 2>$null)
        } catch {
            $schedulerId = ""
        }
        if (-not [string]::IsNullOrEmpty($schedulerId)) {
            Invoke-NativeChecked "docker" (@("compose", "exec", "airflow-scheduler", "python", "-m", "lzd_pipeline.reconstruction.e2e") + $branchArgs) -DockerHint
            break
        }

        Write-Host "  airflow-scheduler chua chay; dung Docker one-shot container." -ForegroundColor Yellow
        $image = ""
        foreach ($candidate in @("lzd/airflow:2.10.5", "lzd/dbt-duckdb:dev", "lzd/airflow:dev", "pipeline-base:py311")) {
            $imageId = (docker images -q $candidate)
            if (-not [string]::IsNullOrEmpty($imageId)) {
                $image = $candidate
                break
            }
        }
        if ([string]::IsNullOrEmpty($image)) {
            throw "Khong co image local phu hop. Hay chay .\scripts\stack.ps1 init/build sau khi Docker registry TLS hoat dong."
        }
        Write-Host "  image: $image" -ForegroundColor Cyan
        $runArgs = @(
            "run", "--rm",
            "--entrypoint", "python",
            "-v", "${Root}:/opt/project",
            "-w", "/opt/project",
            "-e", "PYTHONPATH=/opt/project/src",
            $image,
            "-m", "lzd_pipeline.reconstruction.e2e"
        ) + $branchArgs
        Invoke-NativeChecked "docker" $runArgs -DockerHint
    }

    "snapshot" {
        Write-Section "Ghi reconstruction snapshot vao docs/reconstruction_snapshot"
        $branchArgs = @()
        if (-not [string]::IsNullOrEmpty($Target)) {
            if ($Target -notin @("H1", "H2")) {
                throw "Target phai la H1 hoac H2. Vi du: .\scripts\stack.ps1 snapshot H2"
            }
            $branchArgs = @("--branch", $Target)
        }
        if (-not (Test-Path ".tmp")) { New-Item -ItemType Directory -Force ".tmp" | Out-Null }
        $env:TEMP = (Resolve-Path ".tmp").Path
        $env:TMP = $env:TEMP
        $env:PYTHONPATH = "src"
        Invoke-NativeChecked "python" (@("-m", "lzd_pipeline.reconstruction.snapshot") + $branchArgs)
    }

    "track-a" {
        Write-Section "Track A real backfill tu data/full_trainset.csv"
        if (-not (Test-Path ".tmp")) { New-Item -ItemType Directory -Force ".tmp" | Out-Null }
        $env:TEMP = (Resolve-Path ".tmp").Path
        $env:TMP = $env:TEMP
        $env:PYTHONPATH = "src"

        if ([string]::IsNullOrEmpty($Target)) {
            $runArgs = @("--limit", "1000")
        } elseif ($Target -eq "all") {
            $runArgs = @("--all")
            Write-Host "  Chay full train co the sinh hang chuc trieu event va file rat lon." -ForegroundColor Yellow
        } else {
            $parsed = 0
            if (-not [int]::TryParse($Target, [ref]$parsed) -or $parsed -lt 1) {
                throw "Target phai la so dong hoac 'all'. Vi du: .\scripts\stack.ps1 track-a 1000"
            }
            $runArgs = @("--limit", "$parsed")
        }

        Invoke-NativeChecked "python" (
            @("-m", "lzd_pipeline.reconstruction.track_a_batch") + $runArgs + @("--verify-limit", "50")
        )
    }

    "test" {
        Write-Section "Unit test (khong can Docker)"
        # KHONG tro TEMP vao .tmp cho test: pytest tao basetemp bang
        # mkdir(mode=0o700), va tu Python 3.13 Windows moi that su ap dung mode do
        # -> DACL chi con SYSTEM + Administrators + OWNER RIGHTS. Neu thu muc
        # pytest-of-<user> da bi mot tai khoan khac tao truoc, user hien tai mat
        # sach quyen va moi test dung tmp_path chet voi WinError 5.
        # Test khong sinh file lon nen dung TEMP he thong la du.
        Invoke-NativeChecked "python" @("-m", "pytest", "tests", "-q", "-p", "no:cacheprovider")
    }
}
