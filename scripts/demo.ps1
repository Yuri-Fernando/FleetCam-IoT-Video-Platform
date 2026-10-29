param([int]$Seed = 0, [int]$OfflineSeconds = 75)
$ErrorActionPreference = 'Stop'
if ($Seed -eq 0) { $Seed = Get-Random -Minimum 10000 -Maximum 999999 }
if ($OfflineSeconds -lt 0 -or $OfflineSeconds -gt 3600) { throw 'OfflineSeconds deve estar entre 0 e 3600.' }
function Invoke-Checked([string]$Label, [string[]]$Arguments) {
    Write-Host "`n$Label"
    & py -3.10 @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Label falhou com código $LASTEXITCODE" }
}
Write-Host "Iniciando laboratório FleetCam; seed $Seed."
& docker compose up --build -d
if ($LASTEXITCODE -ne 0) { throw 'Falha ao iniciar o Compose.' }
$deadline = (Get-Date).AddSeconds(180)
$ready = $false
while ((Get-Date) -lt $deadline) {
    try { if ((Invoke-RestMethod 'http://127.0.0.1:8080/actuator/health' -TimeoutSec 3).status -eq 'UP') { $ready=$true; break } } catch {}
    Start-Sleep -Seconds 2
}
if (-not $ready) { throw 'API não ficou pronta; consulte docker compose logs api.' }
Invoke-Checked 'Prontidão da API e do armazenamento S3' @('simulator/wait_ready.py')
Invoke-Checked 'Cenário offline e replay' @('simulator/fleet.py','--seed',"$Seed",'--devices','1','--scenario','offline_reconnect','--offline-seconds',"$OfflineSeconds",'--output',"simulator/output/demo-$Seed")
Invoke-Checked 'Falha e recuperação de SD' @('simulator/incident_check.py')
Invoke-Checked 'Evidência e integridade' @('simulator/evidence_check.py')
Invoke-Checked 'Correlação de trace' @('simulator/tracing_check.py')
Invoke-Checked 'Outbox durante indisponibilidade Kafka' @('simulator/fault_check.py')
Write-Host "`nConsole: http://localhost:8081 | Jaeger: http://localhost:16686 | Grafana: http://localhost:3000"
Write-Host "Dados do cenário: simulator/output/demo-$Seed. Leia docs/runbook.md para investigação."
