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
    print(f"🛠️  Creating catalog: {CATALOG_NAME}")
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
            "scope": "PRINCIPAL_ROLE:ALL",
            "credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
            "header.Polaris-Realm": REALM,  # Essential for PyIceberg
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )
    # print(f"name spaces {catalog.list_namespaces()}")
    # catalog.create_namespace("lakehouse")

    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "data", StringType(), required=True),
        identifier_field_ids=[1],  # Enable Upserts
    )
    table_id = "lakehouse.managed_table"
    table = catalog.create_table(table_id, schema)

    print(f"tables: {catalog.list_tables('lakehouse')}")
    print(table.scan().to_pandas())


if __name__ == "__main__":
    # bootstrap()
    run_test()
