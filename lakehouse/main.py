import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

# Configuration
POLARIS_URL = "http://localhost:8181"
CLIENT_ID = "root"  # Matches the 2nd param in your bootstrap -c
CLIENT_SECRET = "secret"  # Matches the 3rd param in your bootstrap -c
REALM = "default-realm"
CATALOG_NAME = "managed_catalog"


def bootstrap_polaris_catalog():
    # ... (previous token and catalog creation code) ...
    mgmt_headers = {
        "Polaris-Realm": REALM,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # 1. We need to grant the 'CATALOG_MANAGE_CONTENT' privilege
    # to the 'catalog_admin' role.
    privilege_url = f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG_NAME}/roles/catalog_admin/grants"

    # This payload grants the power to actually 'vending' credentials for S3
    privilege_payload = {"privilege": "CATALOG_MANAGE_CONTENT"}

    requests.post(privilege_url, json=privilege_payload, headers=mgmt_headers)

    # 2. Ensure root is still assigned to this role
    grant_role_url = f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG_NAME}/roles/catalog_admin/grants"
    requests.post(grant_role_url, json={"principal": "root"}, headers=mgmt_headers)

    print("✅ Granted CATALOG_MANAGE_CONTENT to catalog_admin.")


def bootstrap_polaris():
    print(f"🚀 Initializing Polaris Management for Realm: {REALM}")

    # --- 1. GET MANAGEMENT TOKEN ---
    token_url = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"
    auth_headers = {
        "Polaris-Realm": REALM,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    token_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "PRINCIPAL_ROLE:ALL",
    }

    token_resp = requests.post(token_url, headers=auth_headers, data=token_data)
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]

    # Standard headers for all subsequent management calls
    mgmt_headers = {
        "Authorization": f"Bearer {token}",
        "Polaris-Realm": REALM,
        "Content-Type": "application/json",
    }

    # --- 2. CREATE CATALOG ---
    # We check if it exists first to avoid 409 Conflict errors
    catalog_base_url = f"{POLARIS_URL}/api/management/v1/catalogs"
    existing_catalogs = (
        requests.get(catalog_base_url, headers=mgmt_headers).json().get("catalogs", [])
    )

    if any(c["name"] == CATALOG_NAME for c in existing_catalogs):
        print(f"Catalog '{CATALOG_NAME}' already exists. Skipping creation.")
    else:
        print(f"Creating Managed Catalog: {CATALOG_NAME}")
        catalog_payload = {
            "catalog": {
                "name": CATALOG_NAME,
                "type": "INTERNAL",
                "properties": {
                    "default-base-location": "s3://warehouse/",
                    "s3.endpoint": "http://minio:9000",
                    "s3.region": "us-east-1",
                    "s3.access-key-id": "admin",
                    "s3.secret-access-key": "password",
                    "s3.path-style-access": "true",
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": ["s3://warehouse/"],
                    "region": "us-east-1",
                },
            }
        }
        requests.post(
            catalog_base_url, json=catalog_payload, headers=mgmt_headers
        ).raise_for_status()

    # --- 3. GRANT STORAGE & METADATA PRIVILEGES ---
    # This addresses the 'CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION' error
    # In Polaris 1.2.0, use PUT to assign privileges to a Catalog Role
    role_priv_url = (
        f"{catalog_base_url}/{CATALOG_NAME}/catalog-roles/catalog_admin/grants"
    )

    # We grant 'CATALOG_MANAGE_CONTENT' which is an umbrella for all data ops
    privilege_payload = {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}

    print("🔐 Granting CATALOG_MANAGE_CONTENT (Data Writing) to 'catalog_admin'...")
    priv_resp = requests.put(
        role_priv_url, json=privilege_payload, headers=mgmt_headers
    )
    if priv_resp.status_code not in [200, 201, 204]:
        print(f"⚠️  Privilege grant warning: {priv_resp.text}")

    # --- 4. LINK PRINCIPAL TO ROLE ---
    # Bridge the 'root' principal to the 'catalog_admin' role
    grant_principal_url = (
        f"{catalog_base_url}/{CATALOG_NAME}/roles/catalog_admin/grants"
    )
    grant_data = {"principal": CLIENT_ID}  # root

    requests.post(grant_principal_url, json=grant_data, headers=mgmt_headers)

    print(f"✅ Bootstrap finished. Catalog '{CATALOG_NAME}' is ready for PyIceberg.")


def bootstrap():
    print(f"Requesting OAuth token for realm: {REALM}")
    token_url = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"

    # --- THE FIX: Include the Realm header and form data ---
    headers = {
        "Polaris-Realm": REALM,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "PRINCIPAL_ROLE:ALL",
    }

    response = requests.post(token_url, headers=headers, data=data)

    if response.status_code != 200:
        print(f"Token request failed: {response.status_code}")
        print(f"Response Body: {response.text}")
        response.raise_for_status()

    token = response.json()["access_token"]
    print("Token acquired.")

    # 2. Create Catalog
    print(f"Creating catalog: {CATALOG_NAME}")
    mgmt_headers = {
        "Authorization": f"Bearer {token}",
        "Polaris-Realm": REALM,
        "Content-Type": "application/json",
    }

    catalog_json = {
        "catalog": {
            "name": CATALOG_NAME,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": "s3://warehouse/",
                "s3.endpoint": "http://minio:9000",
                "s3.region": "us-east-1",
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "password",
                "s3.path-style-access": "true",
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": ["s3://warehouse/"],
                "region": "us-east-1",
            },
        }
    }

    resp = requests.post(
        f"{POLARIS_URL}/api/management/v1/catalogs",
        json=catalog_json,
        headers=mgmt_headers,
    )

    if resp.status_code == 201:
        print(f"Catalog '{CATALOG_NAME}' created.")
    else:
        print(f"Catalog status: {resp.status_code} - {resp.text}")


def run_test():
    print("Connecting PyIceberg to Polaris...")
    catalog = load_catalog(
        "polaris",
        **{
            "uri": f"{POLARIS_URL}/api/catalog",
            "warehouse": CATALOG_NAME,
            "credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
            "scope": "PRINCIPAL_ROLE:ALL",
            "header.Polaris-Realm": REALM,
            # "header.X-Iceberg-Access-Delegation": "vended-credentials",
            "header.X-Iceberg-Access-Delegation": "none",
            # Fallback local S3 config
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
    print(f"name spaces {catalog.list_namespaces()}")
    # catalog.create_namespace("lakehouse")

    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "data", StringType(), required=True),
        identifier_field_ids=[1],  # Enable Upserts
    )
    table_id = "lakehouse.managed_table"
    table = catalog.create_table(table_id, schema)

    print(f"tables: {catalog.list_tables('lakehouse')}")
    # print(table.scan().to_pandas())


if __name__ == "__main__":
    # bootstrap()
    # bootstrap_polaris()
    # bootstrap_polaris_catalog()
    run_test()
