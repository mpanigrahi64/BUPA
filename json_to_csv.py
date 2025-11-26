import pandas as pd
import json
import re
import numpy as np
import io
from openpyxl.styles import Alignment
from google.cloud import storage
import os

# --- Configuration ---
PROJECT_ID = "bupa-pai-dev-653687"
GCS_INPUT_JSON_PREFIX = "gs://bupa-policy-doc-ingest/output/IHHP_cleaned/" # <-- Use your cleaned JSON folder
GCS_OUTPUT_EXCEL_PATH = "gs://bupa-policy-doc-ingest/output/IHHP/IHHP_extracts_final.xlsx"

storage_client = storage.Client(project=PROJECT_ID)

# --- Helper Functions (From your original script) ---

def clean_text(value):
    """Simple text cleaning helper"""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None

def normalize_newlines(text):
    """Converts all newline variants to standard Python '\n' for Excel."""
    if not text or pd.isna(text):
        return text
    return re.sub(r'\r\n|\r', '\n', str(text))

def create_record(entity, prop1=None, prop2=None):
    # This structure must match the columns you want in your Excel file
    record = {
        'source_file': entity.get('source_file_hint'), # Added a field to track the source file
        'entity_type': entity.get('type'),
        'entity_mentionText': clean_text(entity.get('mentionText')),
        
        
        'prop1_type': prop1.get('type') if prop1 else None,
        'prop1_mentionText': clean_text(prop1.get('mentionText')) if prop1 else None,
        
    }
    # Ensure all string values are cleaned of newlines for initial processing
    for key, value in record.items():
        if isinstance(value, str):
            record[key] = normalize_newlines(value)
    return record

# --- Main GCS to Excel Conversion Logic ---

def convert_gcs_jsons_to_excel(gcs_input_prefix: str, gcs_output_path: str):
    print(f"Starting Excel conversion from: {gcs_input_prefix}")

    # Parse GCS input URI
    matches = re.match(r"gs://(.*?)/(.*)", gcs_input_prefix)
    if not matches:
        raise ValueError("Invalid GCS input prefix format.")
    input_bucket_name, input_blob_prefix = matches.groups()

    # List all JSON blobs in the input prefix
    blobs = storage_client.list_blobs(input_bucket_name, prefix=input_blob_prefix)
    all_records = []
    file_count = 0

    for blob in blobs:
        if blob.name.endswith('.json'):
            file_count += 1
            print(f"Processing JSON file: {blob.name}")
            try:
                # Download JSON data as bytes and load as dict
                doc_bytes = blob.download_as_bytes()
                data = json.loads(doc_bytes)
                entities_list = data.get("entities", [])

                if not entities_list:
                    print(f"Warning: 'entities' key empty in {blob.name}")
                    continue

                # Process the nested JSON data iteratively
                for entity in entities_list:
                    # Add hint to track which file this entity came from
                    entity['source_file_hint'] = os.path.basename(blob.name) 
                    
                    if entity.get('properties'):
                        for prop1 in entity['properties']:
                            if prop1.get('properties'):
                                for prop2 in prop1['properties']:
                                    all_records.append(create_record(entity, prop1, prop2))
                            else:
                                all_records.append(create_record(entity, prop1))
                    else:
                        all_records.append(create_record(entity))

            except Exception as e:
                print(f"Error processing blob {blob.name}: {e}")
                continue

    if not all_records:
        print(f"No records found across {file_count} files. Exiting.")
        return

    df_final = pd.DataFrame(all_records).replace({None: np.nan})

    # --- Write to XLSX with Formatting to an In-Memory Buffer ---
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_final.to_excel(writer, index=False, sheet_name='Data')
        
        # Apply Excel formatting (from your original script)
        worksheet = writer.sheets['Data']
        wrap_alignment = Alignment(wrap_text=True, vertical='top')
        text_cols_to_wrap = ['entity_mentionText', 'prop1_mentionText', 'prop2_mentionText', 'source_file']
        
        col_indices_to_wrap = [
            df_final.columns.get_loc(col_name) + 1 
            for col_name in text_cols_to_wrap if col_name in df_final.columns
        ]

        for row in worksheet.iter_rows():
            for cell in row:
                if cell.column in col_indices_to_wrap:
                    cell.alignment = wrap_alignment

    # --- Upload the BytesIO buffer to GCS ---
    output.seek(0) # Rewind the buffer

    # Parse GCS output URI
    matches_out = re.match(r"gs://(.*?)/(.*)", gcs_output_path)
    if not matches_out:
        raise ValueError("Invalid GCS output path format.")
    output_bucket_name, output_blob_name = matches_out.groups()

    output_bucket = storage_client.bucket(output_bucket_name)
    output_blob = output_bucket.blob(output_blob_name)
    
    output_blob.upload_from_file(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    print(f"\nSuccessfully converted {file_count} JSON files to XLSX.")
    print(f"Output saved to: {gcs_output_path}")
    print(f"Total rows generated: {len(df_final)}")


# --- Run the function ---
if __name__ == "__main__":
    convert_gcs_jsons_to_excel(GCS_INPUT_JSON_PREFIX, GCS_OUTPUT_EXCEL_PATH)
