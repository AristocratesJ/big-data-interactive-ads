Write-Host "Stopping NiFi data ingestion flows..." -ForegroundColor Yellow

# Disable SSL certificate validation for self-signed certs (PowerShell 5.1 compatible)
add-type @"
    using System.Net;
    using System.Security.Cryptography.X509Certificates;
    public class TrustAllCertsPolicy : ICertificatePolicy {
        public bool CheckValidationResult(ServicePoint srvPoint, X509Certificate certificate, WebRequest request, int certificateProblem) {
            return true;
        }
    }
"@
[System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12

# NiFi connection details
$nifiUrl = "https://localhost:8443/nifi-api"
$username = "admin"
$password = "adminadmin123"

# Step 1: Authenticate
Write-Host "`nAuthenticating with NiFi..." -ForegroundColor Yellow
try {
    $authResponse = Invoke-RestMethod -Uri "$nifiUrl/access/token" -Method Post -Body "username=$username&password=$password" -ErrorAction Stop
    $token = $authResponse
    $headers = @{ "Authorization" = "Bearer $token" }
    Write-Host " Authenticated successfully" -ForegroundColor Green
} catch {
    Write-Host " Authentication failed: $_" -ForegroundColor Red
    exit 1
}

# Step 2: Get root process group
Write-Host "`nGetting process group..." -ForegroundColor Yellow
try {
    $rootFlow = Invoke-RestMethod -Uri "$nifiUrl/flow/process-groups/root" -Method Get -Headers $headers -ErrorAction Stop
    $rootId = $rootFlow.processGroupFlow.id
    Write-Host " Found root process group: $rootId" -ForegroundColor Green
} catch {
    Write-Host " Failed to get process group: $_" -ForegroundColor Red
    exit 1
}

# Step 3: Get all processors
Write-Host "`nGetting processors..." -ForegroundColor Yellow
try {
    $processorsResponse = Invoke-RestMethod -Uri "$nifiUrl/process-groups/$rootId/processors" -Method Get -Headers $headers -ErrorAction Stop
    $processors = $processorsResponse.processors
    Write-Host " Found $($processors.Count) processors" -ForegroundColor Green
} catch {
    Write-Host " Failed to get processors: $_" -ForegroundColor Red
    exit 1
}

# Step 4: Stop all processors
Write-Host "`nStopping processors..." -ForegroundColor Yellow
$successCount = 0
foreach ($proc in $processors) {
    $procId = $proc.id
    $procName = $proc.component.name
    $revision = $proc.revision.version
    try {
        $body = @{ revision = @{ version = $revision }; component = @{ id = $procId; state = "STOPPED" } } | ConvertTo-Json -Depth 10
        $stopResponse = Invoke-RestMethod -Uri "$nifiUrl/processors/$procId" -Method Put -Headers $headers -Body $body -ContentType "application/json" -ErrorAction Stop
        Write-Host "   Stopped: $procName" -ForegroundColor Green
        $successCount++
    } catch {
        Write-Host "   Failed to stop $procName : $_" -ForegroundColor Red
    }
}

Write-Host "`n$successCount/$($processors.Count) processors stopped successfully" -ForegroundColor Cyan
Write-Host "`nData ingestion flows are now stopped" -ForegroundColor Green
Write-Host "To restart: .\scripts\run_nifi_flows.ps1" -ForegroundColor Yellow