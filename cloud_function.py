import os
import re
import time
import logging
import google.auth
from googleapiclient.discovery import build
import functions_framework

# ======== ENV VARS you set in the Cloud Function Console ========
# PROJECT_ID           
# REGION               
# TEMPLATE_PATH        
# OUTPUT_TABLE         
# TEMP_LOCATION        
# JSON_PATH            
# UDF_GCS_PATH         
# UDF_FUNC_NAME        
# (optional) FILE_SUFFIX  e.g. .csv
# (optional) DELIMITER    e.g. ,
# ==================================================

PROJECT_ID   = os.environ.get("PROJECT_ID")
REGION       = os.environ.get("REGION", "us-central1")
TEMPLATE_PATH= os.environ.get("TEMPLATE_PATH")
OUTPUT_TABLE = os.environ.get("OUTPUT_TABLE")
TEMP_LOCATION= os.environ.get("TEMP_LOCATION")
JSON_PATH    = os.environ.get("JSON_PATH")
UDF_GCS_PATH = os.environ.get("UDF_GCS_PATH")
UDF_FUNC_NAME= os.environ.get("UDF_FUNC_NAME", "transform")
FILE_SUFFIX  = os.environ.get("FILE_SUFFIX", "")
DELIMITER    = os.environ.get("DELIMITER", "")

def _sanitize_job_name(name: str) -> str:
    s = re.sub(r'[^a-z0-9-]+', '-', (name or "").lower()).strip("-")
    if not s or not s[0].isalpha():
        s = "job-" + s
    return s[:50]

def _require(*pairs):
    missing = [k for k, v in pairs if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

@functions_framework.cloud_event
def start_dataflow(cloud_event):
    """GCS 'finalized' → launch Classic prebuilt Dataflow template."""
    try:
        data = cloud_event.data or {}
        bucket = data.get("bucket")
        name   = data.get("name")

        if not bucket or not name or name.endswith("/"):
            logging.info("Skipping folder or malformed event.")
            return "Skipped", 200
        if FILE_SUFFIX and not name.endswith(FILE_SUFFIX):
            logging.info(f"Skipping {name}: suffix filter {FILE_SUFFIX}")
            return "Skipped", 200

        _require(
            ("PROJECT_ID", PROJECT_ID),
            ("REGION", REGION),
            ("TEMPLATE_PATH", TEMPLATE_PATH),
            ("OUTPUT_TABLE", OUTPUT_TABLE),
            ("TEMP_LOCATION", TEMP_LOCATION),
            ("JSON_PATH", JSON_PATH),
            ("UDF_GCS_PATH", UDF_GCS_PATH),
            ("UDF_FUNC_NAME", UDF_FUNC_NAME),
        )

        input_file = f"gs://{bucket}/{name}"
        base = _sanitize_job_name(os.path.splitext(os.path.basename(name))[0])
        job_name = f"{base}-{int(time.time())}"

        creds, proj = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        project_id = PROJECT_ID or proj
        df = build("dataflow", "v1b3", credentials=creds, cache_discovery=False)

        # Classic template parameters for GCS_Text_to_BigQuery
        params = {
            "inputFilePattern": input_file,
            "JSONPath": JSON_PATH,
            "outputTable": OUTPUT_TABLE,
            "javascriptTextTransformGcsPath": UDF_GCS_PATH,
            "javascriptTextTransformFunctionName": UDF_FUNC_NAME,
            "bigQueryLoadingTemporaryDirectory": TEMP_LOCATION,
        }
        if DELIMITER:
            params["delimiter"] = DELIMITER  # optional override

        body = {
            "jobName": job_name,
            "parameters": params,
            "environment": {"tempLocation": TEMP_LOCATION}
        }

        resp = df.projects().locations().templates().launch(
            projectId=project_id,
            location=REGION,
            gcsPath=TEMPLATE_PATH,
            body=body
        ).execute()

        logging.info(f"Launched Dataflow job: {resp}")
        return "OK", 200

    except Exception as e:
        logging.exception(f"Failed to launch Dataflow: {e}")
        return f"Error: {e}", 500
(extractor) kelvinaliche@Kelvins-MacBook-Air Data Engineering % 
