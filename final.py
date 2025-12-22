


#---------------------------------------------------------------------------------------------

import requests
import pandas as pd
import os
import json
from msal import ConfidentialClientApplication

# =====================================================
# CONFIGURATION
# =====================================================

# TARGET WORKSPACE (DATA + REPORT WILL GO HERE)

TENANT_ID = os.getenv("POWERBI_TENANT_ID")
CLIENT_ID = os.getenv("POWERBI_CLIENT_ID")
CLIENT_SECRET = os.getenv("POWERBI_CLIENT_SECRET")

WORKSPACE_ID = os.getenv("POWERBI_WORKSPACE_ID")
TEMPLATE_WORKSPACE_ID = os.getenv("POWERBI_TEMPLATE_WORKSPACE_ID")
TEMPLATE_REPORT_ID = os.getenv("POWERBI_TEMPLATE_REPORT_ID")
# EXCEL FILE
EXCEL_PATH = r"C:\Users\GarrajuNaralasetti(Q\Downloads\updated_candidate_data.xlsx"

DATASET_NAME = "Excel_Push_Dataset"
TABLE_NAME = "MainTable"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
API_ROOT = "https://api.powerbi.com/v1.0/myorg"

# =====================================================
# STEP 1: AUTHENTICATION
# =====================================================

print("🔐 Authenticating with Power BI...")

app = ConfidentialClientApplication(
    CLIENT_ID,
    authority=AUTHORITY,
    client_credential=CLIENT_SECRET
)

token = app.acquire_token_for_client(scopes=SCOPE)

if "access_token" not in token:
    raise Exception(f"Authentication failed: {token}")

access_token = token["access_token"]

HEADERS = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

print("✅ Authentication successful")

# =====================================================
# STEP 2: READ EXCEL
# =====================================================

if not os.path.exists(EXCEL_PATH):
    raise FileNotFoundError(f"Excel not found: {EXCEL_PATH}")

print("📊 Reading Excel file...")
df = pd.read_excel(EXCEL_PATH)

# =====================================================
# STEP 3: BUILD DATASET SCHEMA
# =====================================================

def map_dtype(dtype):
    dtype = str(dtype).lower()
    if "int" in dtype:
        return "Int64"
    if "float" in dtype:
        return "Double"
    if "datetime" in dtype:
        return "DateTime"
    return "String"

columns = [
    {"name": col, "dataType": map_dtype(df[col].dtype)}
    for col in df.columns
]

dataset_payload = {
    "name": DATASET_NAME,
    "defaultMode": "Push",
    "tables": [
        {
            "name": TABLE_NAME,
            "columns": columns
        }
    ]
}

# =====================================================
# STEP 4: CREATE DATASET
# =====================================================

print("📦 Creating dataset...")

dataset_response = requests.post(
    f"{API_ROOT}/groups/{WORKSPACE_ID}/datasets",
    headers=HEADERS,
    json=dataset_payload
)

dataset_response.raise_for_status()
DATASET_ID = dataset_response.json()["id"]

print("✅ Dataset created:", DATASET_ID)

# =====================================================
# STEP 5: DATA SANITIZATION
# =====================================================

for col in df.columns:
    if pd.api.types.is_datetime64_any_dtype(df[col]):
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

df = df.astype(object).where(pd.notnull(df), None)

# =====================================================
# STEP 6: PUSH DATA
# =====================================================

print("⬆️ Pushing data...")

rows_payload = {
    "rows": df.to_dict(orient="records")
}

push_response = requests.post(
    f"{API_ROOT}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/tables/{TABLE_NAME}/rows",
    headers=HEADERS,
    json=rows_payload
)

push_response.raise_for_status()

print("✅ Data pushed successfully")

# =====================================================
# STEP 7: CLONE TEMPLATE REPORT
# =====================================================

print("📄 Cloning template report...")

clone_payload = {
    "name": "blankreport_withdataset",
    "targetWorkspaceId": WORKSPACE_ID,
    "targetModelId": DATASET_ID
}

clone_response = requests.post(
    f"{API_ROOT}/groups/{TEMPLATE_WORKSPACE_ID}/reports/{TEMPLATE_REPORT_ID}/Clone",
    headers=HEADERS,
    json=clone_payload
)

if clone_response.status_code != 200:
    print("⚠️ Clone failed (likely due to Push dataset limitation)")
    print(clone_response.text)
else:
    NEW_REPORT_ID = clone_response.json()["id"]
    print("✅ Report cloned:", NEW_REPORT_ID)

    # =====================================================
    # STEP 8: REBIND (SAFE)
    # =====================================================

    rebind_payload = {"datasetId": DATASET_ID}

    requests.post(
        f"{API_ROOT}/groups/{WORKSPACE_ID}/reports/{NEW_REPORT_ID}/Rebind",
        headers=HEADERS,
        json=rebind_payload
    )

    print("✅ Report rebound")

# =====================================================
# DONE
# =====================================================

print("\n🎉 PIPELINE COMPLETED")
print("➡️ Dataset created")
print("➡️ Data pushed")
print("➡️ Report cloned (if supported)")




