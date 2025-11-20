import re
import json
from typing import Optional
from google.cloud import documentai, storage
from google.api_core.exceptions import RetryError, InternalServerError
from google.api_core.client_options import ClientOptions

# Import all necessary types from the consolidated types module
from google.cloud.documentai_v1 import types
from google.cloud.documentai_v1.types.document import Document


project_id = "bupa-pai-dev-653687"
location = "us" # Format is "us" or "eu"
processor_id = "fdf315ce0df23208" # Create processor before running sample
gcs_output_uri = "gs://bupa-policy-doc-ingest/output/bupa-service/" #Must end with a trailing slash `/`. Format: gs://bucket/directory/subdirectory/
gcs_input_prefix = "gs://bupa-policy-doc-ingest/doc-01/pdf/58488-BIN_BHP2.0_Member_Guide_MAT_EN_DEC25.pdf"

FIELDS_TO_REMOVE = [
    'pageRefs', 'textAnchor', 'boundingPoly', 'textSegments', 'pageAnchor', 
    'detectedLanguages', 'layout', 'detectedBreak', 'dimension', 'image', 
    'tables', 'blocks', 'lines', 'tokens', 'pages', 'documentLayout'
]
storage_client = storage.Client(project=project_id)

def upload_dict_as_file(bucket_name: str, blob_name: str, data: dict):
    """Uploads a dict as a JSON blob."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    metadata = json.dumps(data)
    blob.upload_from_string(metadata, content_type="application/json")
    print(f"Uploaded JSON to gs://{bucket_name}/{blob_name} ({len(metadata)} chars)")
    

def remove_fields_recursive(data, fields):
    """
    Recursively removes specified fields from a dictionary or list of dictionaries.
    Modifies the dictionary in place.
    """
    if isinstance(data, dict):
        # Remove top-level occurrence of the field if present
        for field in fields:
            data.pop(field, None)
        # Recurse into nested values
        for key, value in list(data.items()): # Use list() to allow modification during iteration
            remove_fields_recursive(value, fields)
    elif isinstance(data, list):
        # Recurse into list items
        for item in data:
            remove_fields_recursive(item, fields)
    
def reconstruct_mention_text(doc_text, text_segments_list):
    """
    Reconstructs the full text snippet for an entity by sorting and joining
    its text segments from the RAW JSON format. Safely handles missing keys/data.
    """
    text = ''
    
    if not isinstance(text_segments_list, list):
        return text.strip()

    # Sort segments by converting the 'startIndex' string value to an integer
    segments = sorted(
        text_segments_list, 
        key=lambda x: int(x.get('startIndex', '0'))
    )
    
    for segment in segments:
        try:
            start = int(segment.get('startIndex', '0'))
            # Default end index to the start index if missing (for zero-length segments)
            end = int(segment.get('endIndex', str(start))) 
        except ValueError:
            continue # Skip this segment if indices are invalid

        # Ensure indices are within bounds
        if 0 <= start <= end <= len(doc_text):
            text += doc_text[start:end]

    return text.strip()

def process_and_upload_docai_json(bucket_name, source_blob_name, destination_blob_name):
    """
    Downloads raw Document AI JSON file, corrects mentionText ordering, 
    and uploads the updated file to GCS.
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        doc = json.loads(blob.download_as_bytes())
    except Exception as e:
        print(f"Error downloading or parsing JSON file {source_blob_name}: {e}")
        return


    doc_text = doc.get("text", "")

    def correct_entities_recursive(entity_list):
        for entity in entity_list:
            text_segments_list = entity.get("textAnchor", {}).get("textSegments", [])
            
            if text_segments_list and doc_text:
                updated_mention_text = reconstruct_mention_text(doc_text, text_segments_list)
                entity["mentionText"] = updated_mention_text
            
            if "properties" in entity:
                correct_entities_recursive(entity["properties"])

  
    correct_entities_recursive(doc.get("entities", []))
    
    
    print(f"Removing unwanted fields: {FIELDS_TO_REMOVE}")
    remove_fields_recursive(doc, FIELDS_TO_REMOVE)
    
    print(f"Uploading Json File....")
    upload_dict_as_file(bucket_name, destination_blob_name, doc)


def batch_process_documents(
    project_id: str = project_id,
    location: str = location,
    processor_id: str = processor_id,
    gcs_output_uri: str = gcs_output_uri,
    processor_version_id: Optional[str] = None,
    gcs_input_uri: Optional[str] = None, # Using prefix for your use case
    input_mime_type: Optional[str] = None,
    gcs_input_prefix: Optional[str] = gcs_input_prefix, # Using prefix for your use case
    field_mask: Optional[str] = None,  # Optional: use "text,entities" to ensure entities are returned
    timeout: int = 1400,
) -> None:
    """
    Orchestrates the batch processing using the Document AI client.
    """
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)
 
    # --- Input Configuration (Using GCS Prefix as requested) ---
    if not gcs_input_prefix:
         raise ValueError("gcs_input_prefix must be provided for directory processing.")
 
    gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_prefix)
    input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)
 
    # --- Output Configuration ---
    gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
        gcs_uri=gcs_output_uri, field_mask=field_mask
    )
    output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)
 
    # --- Processor Path ---
    if processor_version_id:
        name = client.processor_version_path(
            project_id, location, processor_id, processor_version_id
        )
    else:
        name = client.processor_path(project_id, location, processor_id)
 
    request = documentai.BatchProcessRequest(
        name=name,
        input_documents=input_config,
        document_output_config=output_config,
    )
 
    # --- Run Operation ---
    print(f"Sending batch processing request for processor: {name}")
    operation = client.batch_process_documents(request)
 
    try:
        print(f"Waiting for operation {operation.operation.name} to complete...")
        operation.result(timeout=timeout)
    except (RetryError, InternalServerError) as e:
        print(f"Batch Process Operation failed: {e.message}")
        return
 
    # --- Process Results ---
    metadata = documentai.BatchProcessMetadata(operation.metadata)
 
    if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
        raise ValueError(f"Batch Process Failed: {metadata.state_message}")
 
    storage_client = storage.Client()
    print("Operation SUCCEEDED. Starting output file retrieval:")
 
    for process in list(metadata.individual_process_statuses):
        matches = re.match(r"gs://(.*?)/(.*)", process.output_gcs_destination)
        if not matches:
            continue
 
        output_bucket, output_prefix = matches.groups()
        output_blobs = storage_client.list_blobs(output_bucket, prefix=output_prefix)
 
        for blob in output_blobs:
            if blob.content_type != "application/json":
                continue
 
            # Download JSON File and convert to Document Object
            print(f"\n--- Processing results from file: {blob.name} ---")
            source_blob = blob.name
            destination_blob = source_blob.replace('.json', '_cleaned_sorted.json')
            
            print(f"Starting processing for Json Files")
            process_and_upload_docai_json(
                bucket_name=output_bucket,
                source_blob_name=source_blob,
                destination_blob_name=destination_blob
            )
 
# [END documentai_batch_process_document_custom_extractor]
 
 
if __name__ == "__main__":
    print(f"Calling Doc AI")
    batch_process_documents()
 