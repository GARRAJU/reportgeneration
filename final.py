


# #---------------------------------------------------------------------------------------------

# import requests
# import pandas as pd
# import os
# import json
# from msal import ConfidentialClientApplication

# # =====================================================
# # CONFIGURATION
# # =====================================================

# # TARGET WORKSPACE (DATA + REPORT WILL GO HERE)

# TENANT_ID = os.getenv("POWERBI_TENANT_ID")
# CLIENT_ID = os.getenv("POWERBI_CLIENT_ID")
# CLIENT_SECRET = os.getenv("POWERBI_CLIENT_SECRET")

# WORKSPACE_ID = os.getenv("POWERBI_WORKSPACE_ID")
# TEMPLATE_WORKSPACE_ID = os.getenv("POWERBI_TEMPLATE_WORKSPACE_ID")
# TEMPLATE_REPORT_ID = os.getenv("POWERBI_TEMPLATE_REPORT_ID")
# # EXCEL FILE
# EXCEL_PATH = r"C:\Users\GarrajuNaralasetti(Q\Downloads\updated_candidate_data.xlsx"

# DATASET_NAME = "Excel_Push_Dataset"
# TABLE_NAME = "MainTable"

# AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
# SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
# API_ROOT = "https://api.powerbi.com/v1.0/myorg"

# # =====================================================
# # STEP 1: AUTHENTICATION
# # =====================================================

# print("🔐 Authenticating with Power BI...")

# app = ConfidentialClientApplication(
#     CLIENT_ID,
#     authority=AUTHORITY,
#     client_credential=CLIENT_SECRET
# )

# token = app.acquire_token_for_client(scopes=SCOPE)

# if "access_token" not in token:
#     raise Exception(f"Authentication failed: {token}")

# access_token = token["access_token"]

# HEADERS = {
#     "Authorization": f"Bearer {access_token}",
#     "Content-Type": "application/json"
# }

# print("✅ Authentication successful")

# # =====================================================
# # STEP 2: READ EXCEL
# # =====================================================

# if not os.path.exists(EXCEL_PATH):
#     raise FileNotFoundError(f"Excel not found: {EXCEL_PATH}")

# print("📊 Reading Excel file...")
# df = pd.read_excel(EXCEL_PATH)

# # =====================================================
# # STEP 3: BUILD DATASET SCHEMA
# # =====================================================

# def map_dtype(dtype):
#     dtype = str(dtype).lower()
#     if "int" in dtype:
#         return "Int64"
#     if "float" in dtype:
#         return "Double"
#     if "datetime" in dtype:
#         return "DateTime"
#     return "String"

# columns = [
#     {"name": col, "dataType": map_dtype(df[col].dtype)}
#     for col in df.columns
# ]

# dataset_payload = {
#     "name": DATASET_NAME,
#     "defaultMode": "Push",
#     "tables": [
#         {
#             "name": TABLE_NAME,
#             "columns": columns
#         }
#     ]
# }

# # =====================================================
# # STEP 4: CREATE DATASET
# # =====================================================

# print("📦 Creating dataset...")

# dataset_response = requests.post(
#     f"{API_ROOT}/groups/{WORKSPACE_ID}/datasets",
#     headers=HEADERS,
#     json=dataset_payload
# )

# dataset_response.raise_for_status()
# DATASET_ID = dataset_response.json()["id"]

# print("✅ Dataset created:", DATASET_ID)

# # =====================================================
# # STEP 5: DATA SANITIZATION
# # =====================================================

# for col in df.columns:
#     if pd.api.types.is_datetime64_any_dtype(df[col]):
#         df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

# df = df.astype(object).where(pd.notnull(df), None)

# # =====================================================
# # STEP 6: PUSH DATA
# # =====================================================

# print("⬆️ Pushing data...")

# rows_payload = {
#     "rows": df.to_dict(orient="records")
# }

# push_response = requests.post(
#     f"{API_ROOT}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/tables/{TABLE_NAME}/rows",
#     headers=HEADERS,
#     json=rows_payload
# )

# push_response.raise_for_status()

# print("✅ Data pushed successfully")

# # =====================================================
# # STEP 7: CLONE TEMPLATE REPORT
# # =====================================================

# print("📄 Cloning template report...")

# clone_payload = {
#     "name": "blankreport_withdataset",
#     "targetWorkspaceId": WORKSPACE_ID,
#     "targetModelId": DATASET_ID
# }

# clone_response = requests.post(
#     f"{API_ROOT}/groups/{TEMPLATE_WORKSPACE_ID}/reports/{TEMPLATE_REPORT_ID}/Clone",
#     headers=HEADERS,
#     json=clone_payload
# )

# if clone_response.status_code != 200:
#     print("⚠️ Clone failed (likely due to Push dataset limitation)")
#     print(clone_response.text)
# else:
#     NEW_REPORT_ID = clone_response.json()["id"]
#     print("✅ Report cloned:", NEW_REPORT_ID)

#     # =====================================================
#     # STEP 8: REBIND (SAFE)
#     # =====================================================

#     rebind_payload = {"datasetId": DATASET_ID}

#     requests.post(
#         f"{API_ROOT}/groups/{WORKSPACE_ID}/reports/{NEW_REPORT_ID}/Rebind",
#         headers=HEADERS,
#         json=rebind_payload
#     )

#     print("✅ Report rebound")

# # =====================================================
# # DONE
# # =====================================================

# print("\n🎉 PIPELINE COMPLETED")
# print("➡️ Dataset created")
# print("➡️ Data pushed")
# print("➡️ Report cloned (if supported)")

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import requests
import os
import tempfile
from msal import ConfidentialClientApplication
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Power BI Dataset & Report Generator")

# =====================================================
# ENV CONFIG
# =====================================================

TENANT_ID = os.getenv("POWERBI_TENANT_ID")
CLIENT_ID = os.getenv("POWERBI_CLIENT_ID")
CLIENT_SECRET = os.getenv("POWERBI_CLIENT_SECRET")

TEMPLATE_WORKSPACE_ID = os.getenv("POWERBI_TEMPLATE_WORKSPACE_ID")
TEMPLATE_REPORT_ID = os.getenv("POWERBI_TEMPLATE_REPORT_ID")

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

DATASET_NAME = "Excel_Push_Dataset"
TABLE_NAME = "MainTable"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
API_ROOT = "https://api.powerbi.com/v1.0/myorg"

# =====================================================
# REQUEST MODEL
# =====================================================

class ReportRequest(BaseModel):
    container_name: str
    blob_name: str
    target_workspace_id: str

# =====================================================
# AUTH
# =====================================================

def get_access_token():
    app_auth = ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    token = app_auth.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in token:
        raise HTTPException(status_code=401, detail=token)
    return token["access_token"]

# =====================================================
# UTILS
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

def load_dataframe(file_path: str):
    if file_path.endswith(".xlsx"):
        return pd.read_excel(file_path)
    elif file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file format")

# =====================================================
# MAIN API
# =====================================================

@app.post("/generate-report")
def generate_report(req: ReportRequest):

    try:
        # -------------------------------------------------
        # 1. DOWNLOAD FILE FROM BLOB
        # -------------------------------------------------
        blob_service = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )

        blob_client = blob_service.get_blob_client(
            container=req.container_name,
            blob=req.blob_name
        )

        suffix = ".xlsx" if req.blob_name.endswith(".xlsx") else ".csv"

        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(blob_client.download_blob().readall())
            file_path = tmp.name

        df = load_dataframe(file_path)

        # -------------------------------------------------
        # 2. AUTH
        # -------------------------------------------------
        access_token = get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # -------------------------------------------------
        # 3. CREATE DATASET
        # -------------------------------------------------
        columns = [
            {"name": col, "dataType": map_dtype(df[col].dtype)}
            for col in df.columns
        ]

        dataset_payload = {
            "name": DATASET_NAME,
            "defaultMode": "Push",
            "tables": [{
                "name": TABLE_NAME,
                "columns": columns
            }]
        }

        ds_resp = requests.post(
            f"{API_ROOT}/groups/{req.target_workspace_id}/datasets",
            headers=headers,
            json=dataset_payload
        )
        ds_resp.raise_for_status()
        dataset_id = ds_resp.json()["id"]

        # -------------------------------------------------
        # 4. DATA CLEANING
        # -------------------------------------------------
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

        df = df.astype(object).where(pd.notnull(df), None)

        # -------------------------------------------------
        # 5. PUSH DATA
        # -------------------------------------------------
        rows_payload = {"rows": df.to_dict(orient="records")}

        push_resp = requests.post(
            f"{API_ROOT}/groups/{req.target_workspace_id}/datasets/{dataset_id}/tables/{TABLE_NAME}/rows",
            headers=headers,
            json=rows_payload
        )
        push_resp.raise_for_status()

        # -------------------------------------------------
        # 6. CLONE REPORT
        # -------------------------------------------------
        clone_payload = {
            "name": "Generated_Report",
            "targetWorkspaceId": req.target_workspace_id,
            "targetModelId": dataset_id
        }

        clone_resp = requests.post(
            f"{API_ROOT}/groups/{TEMPLATE_WORKSPACE_ID}/reports/{TEMPLATE_REPORT_ID}/Clone",
            headers=headers,
            json=clone_payload
        )

        result = {
            "datasetId": dataset_id,
            "workspaceId": req.target_workspace_id,
            "dataPushed": True
        }

        if clone_resp.status_code == 200:
            report_id = clone_resp.json()["id"]
            result["reportId"] = report_id

            # Rebind
            requests.post(
                f"{API_ROOT}/groups/{req.target_workspace_id}/reports/{report_id}/Rebind",
                headers=headers,
                json={"datasetId": dataset_id}
            )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



