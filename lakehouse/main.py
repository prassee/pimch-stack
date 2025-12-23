import requests
from pyiceberg.catalog import load_catalog

POLARIS_URL = "http://localhost:8181"
CLIENT_ID = "root"
CLIENT_SECRET = "secret"
CATALOG_NAME = "managed_catalog"


def bootstrap():
    """Initializes the Polaris Catalog via Management API."""
    token = requests.post(
        f"{POLARIS_URL}/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    ).json()["access_token"]
    print(token)
    headers = {"Authorization": f"Bearer {token}"}
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
    requests.post(
        f"{POLARIS_URL}/api/management/v1/catalogs", json=catalog_json, headers=headers
    )


def run_managed_test():
    # catalog = load_catalog(
    #     "polaris",
    #     **{
    #         "uri": f"{POLARIS_URL}/api/catalog",
    #         "warehouse": CATALOG_NAME,
    #         "credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
    #         "s3.endpoint": "http://localhost:9000",
    #         "s3.access-key-id": "admin",
    #         "s3.secret-access-key": "password",
    #     },
    # )

    catalog = load_catalog(
        "polaris",
        **{
            "uri": "http://localhost:8181/api/catalog",
            "warehouse": "managed_catalog",
            "credential": "root:secret",
            "header.Polaris-Realm": "default-realm",  # <--- MUST match bootstrap realm
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )

    print("\n--- Managed Catalogs ---")
    for namespace in catalog.list_namespaces():
        print(namespace)

    # # 1. Create Namespace
    # ns = "finance"
    # if ns not in [n[0] for n in catalog.list_namespaces()]:
    #     catalog.create_namespace(ns)

    # # 2. Define Schema with Primary Key for Upserts
    # schema = Schema(
    #     NestedField(1, "order_id", LongType(), required=True),
    #     NestedField(2, "customer", StringType(), required=True),
    #     NestedField(3, "status", StringType(), required=True),
    #     identifier_field_ids=[1],
    # )

    # # 3. Create Managed Table
    # table_id = f"{ns}.orders"
    # if table_id in [f"{t[0]}.{t[1]}" for t in catalog.list_tables(ns)]:
    #     table = catalog.load_table(table_id)
    # else:
    #     table = catalog.create_table(table_id, schema=schema)

    # # 4. Incremental Load (Upsert)
    # print("Writing Batch 1...")
    # df1 = pa.table(
    #     {"order_id": [1, 2], "customer": ["Alice", "Bob"], "status": ["New", "New"]},
    #     schema=schema.as_arrow(),
    # )
    # table.append(df1)

    # print("Writing Incremental Batch 2 (Upserting ID 2)...")
    # df2 = pa.table(
    #     {
    #         "order_id": [2, 3],
    #         "customer": ["Bob", "Charlie"],
    #         "status": ["Shipped", "New"],
    #     },
    #     schema=schema.as_arrow(),
    # )
    # # Native Upsert handles row-level changes automatically
    # table.upsert(df=df2, join_cols=["order_id"])

    # print("\n--- Current Managed Table Data ---")
    # print(table.scan().to_pandas().sort_values("order_id"))


if __name__ == "__main__":
    # bootstrap()
    run_managed_test()
