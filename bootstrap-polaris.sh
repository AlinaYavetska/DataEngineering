#!/usr/bin/env bash
# One-time Polaris bootstrap: catalog + roles for root principal.
set -euo pipefail

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"

echo "[*] Requesting access token..."
ACCESS_TOKEN=$(curl -sS -X POST \
  "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  -d 'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')

if [[ -z "${ACCESS_TOKEN}" || "${ACCESS_TOKEN}" == "null" ]]; then
  echo "ERROR: could not obtain access token" >&2; exit 1
fi
echo "[+] token acquired"

echo "[*] Creating catalog polariscatalog..."
curl -sS -i -X POST \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/catalogs" \
  --json '{
    "name": "polariscatalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse",
      "s3.endpoint": "http://minio:9000",
      "s3.path-style-access": "true",
      "s3.access-key-id": "admin",
      "s3.secret-access-key": "password",
      "s3.region": "dummy-region"
    },
    "storageConfigInfo": {
      "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
      "storageType": "S3",
      "allowedLocations": ["s3://warehouse/*"]
    }
  }' || true

echo "[*] Creating catalog_admin role + grant..."
curl -sS -X PUT \
  "${POLARIS_URL}/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  --json '{"grant":{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}}'

echo "[*] Creating data_engineer principal-role..."
curl -sS -X POST "${POLARIS_URL}/api/management/v1/principal-roles" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  --json '{"principalRole":{"name":"data_engineer"}}' || true

echo "[*] Linking data_engineer -> catalog_admin..."
curl -sS -X PUT \
  "${POLARIS_URL}/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  --json '{"catalogRole":{"name":"catalog_admin"}}'

echo "[*] Granting data_engineer to root principal..."
curl -sS -X PUT "${POLARIS_URL}/api/management/v1/principals/root/principal-roles" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  --json '{"principalRole": {"name":"data_engineer"}}'

echo
echo "[+] Bootstrap complete."
echo "Verify with:"
echo "  curl -X GET ${POLARIS_URL}/api/management/v1/catalogs -H \"Authorization: Bearer \$ACCESS_TOKEN\" | jq"
