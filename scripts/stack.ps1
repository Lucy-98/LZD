<#
.SYNOPSIS
    Dieu khien stack LZD Uplift tren Windows.

.EXAMPLE
    .\scripts\stack.ps1 init        # tao .env + build image (chua chay)
    .\scripts\stack.ps1 up          # bat core + observability
    .\scripts\stack.ps1 up-all      # bat tat ca (them stream + serving + ml)
    .\scripts\stack.ps1 status      # trang thai container + link UI
    .\scripts\stack.ps1 logs airflow-scheduler
    .\scripts\stack.ps1 health      # goi thu tung endpoint
    .\scripts\stack.ps1 redis       # mo redis-cli
    .\scripts\stack.ps1 down        # dung (giu du lieu)
    .\scripts\stack.ps1 reset       # dung + XOA het volume
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("init", "build", "up", "up-all", "down", "reset", "status",
                 "logs", "health", "redis", "duckdb", "test", "ps")]
    [string]$Command = "status",

    [Parameter(Position = 1)]
    [string]$Target = ""
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Write-Section($text) {
    Write-Host ""
    Write-Host "=== $text ===" -ForegroundColor Cyan
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

    "init" {
        Write-Section "Tao .env"
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
        docker compose --profile all build
        Show-Urls
        Write-Host "`nTiep theo: .\scripts\stack.ps1 up" -ForegroundColor Cyan
    }

    "build" { docker compose --profile all build }

    "up" {
        docker compose --profile core --profile obs up -d
        Show-Urls
    }

    "up-all" {
        docker compose --profile all up -d
        Show-Urls
    }

    "down"  { docker compose --profile all down }

    "reset" {
        Write-Host "XOA TOAN BO du lieu (postgres, redis, minio, kafka, grafana)." -ForegroundColor Red
        $ans = Read-Host "Go 'yes' de xac nhan"
        if ($ans -eq "yes") {
            docker compose --profile all down -v
            Remove-Item -Recurse -Force "airflow/logs/*" -ErrorAction SilentlyContinue
            Write-Host "Da reset." -ForegroundColor Green
        } else {
            Write-Host "Huy." -ForegroundColor Yellow
        }
    }

    "ps"     { docker compose --profile all ps }

    "status" {
        Write-Section "Container"
        docker compose --profile all ps --format "table {{.Name}}\t{{.Service}}\t{{.Status}}"
        Show-Urls
    }

    "logs" {
        if ([string]::IsNullOrEmpty($Target)) {
            docker compose --profile all logs -f --tail=100
        } else {
            docker compose logs -f --tail=200 $Target
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

    "redis"  { docker compose exec redis redis-cli }

    "duckdb" {
        docker compose exec airflow-scheduler python -c @"
from lzd_pipeline.common.clients import duckdb_conn
with duckdb_conn(read_only=True) as con:
    print(con.execute('SHOW ALL TABLES').fetchdf())
"@
    }

    "test" {
        Write-Section "Unit test (khong can Docker)"
        python -m pytest tests/ -v
    }
}
