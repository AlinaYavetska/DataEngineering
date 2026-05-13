# Polaris bootstrap (Windows / PowerShell-friendly, no jq needed).
# Виконати ОДИН раз після `podman compose up -d`.

$ErrorActionPreference = "Stop"
$PolarisUrl = "http://localhost:8181"

Write-Host "[*] Requesting access token..." -ForegroundColor Cyan
$tokenResp = Invoke-RestMethod -Method Post -Uri "$PolarisUrl/api/catalog/v1/oauth/tokens" `
    -ContentType "application/x-www-form-urlencoded" `
    -Body "grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL"
$Token = $tokenResp.access_token
$Hdr = @{ "Authorization" = "Bearer $Token" }
Write-Host "[+] Token acquired" -ForegroundColor Green

Write-Host "[*] Creating catalog 'polariscatalog'..." -ForegroundColor Cyan
$catBody = @{
    name = "polariscatalog"
    type = "INTERNAL"
    properties = @{
        "default-base-location" = "s3://warehouse"
        "s3.endpoint" = "http://minio:9000"
        "s3.path-style-access" = "true"
        "s3.access-key-id" = "admin"
        "s3.secret-access-key" = "password"
        "s3.region" = "dummy-region"
    }
    storageConfigInfo = @{
        roleArn = "arn:aws:iam::000000000000:role/minio-polaris-role"
        storageType = "S3"
        allowedLocations = @("s3://warehouse/*")
    }
} | ConvertTo-Json -Depth 6
try {
    Invoke-RestMethod -Method Post -Uri "$PolarisUrl/api/management/v1/catalogs" -Headers $Hdr -ContentType "application/json" -Body $catBody | Out-Null
    Write-Host "[+] Catalog created" -ForegroundColor Green
} catch { Write-Host "    (catalog may already exist — continuing)" -ForegroundColor Yellow }

Write-Host "[*] Granting catalog_admin role privileges..." -ForegroundColor Cyan
Invoke-RestMethod -Method Put -Uri "$PolarisUrl/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants" `
    -Headers $Hdr -ContentType "application/json" `
    -Body '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' | Out-Null

Write-Host "[*] Creating principal-role 'data_engineer'..." -ForegroundColor Cyan
try {
    Invoke-RestMethod -Method Post -Uri "$PolarisUrl/api/management/v1/principal-roles" `
        -Headers $Hdr -ContentType "application/json" `
        -Body '{"principalRole":{"name":"data_engineer"}}' | Out-Null
} catch { Write-Host "    (role may already exist)" -ForegroundColor Yellow }

Write-Host "[*] Linking data_engineer -> catalog_admin..." -ForegroundColor Cyan
Invoke-RestMethod -Method Put -Uri "$PolarisUrl/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog" `
    -Headers $Hdr -ContentType "application/json" `
    -Body '{"catalogRole":{"name":"catalog_admin"}}' | Out-Null

Write-Host "[*] Granting data_engineer to root principal..." -ForegroundColor Cyan
Invoke-RestMethod -Method Put -Uri "$PolarisUrl/api/management/v1/principals/root/principal-roles" `
    -Headers $Hdr -ContentType "application/json" `
    -Body '{"principalRole":{"name":"data_engineer"}}' | Out-Null

Write-Host ""
Write-Host "[+] Bootstrap complete. Verify catalog:" -ForegroundColor Green
Invoke-RestMethod -Method Get -Uri "$PolarisUrl/api/management/v1/catalogs" -Headers $Hdr | ConvertTo-Json -Depth 6
