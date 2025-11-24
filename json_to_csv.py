import pandas as pd
import json
import os
import re
import numpy as np # Needed for np.nan
import io          # Needed for in-memory buffer
from openpyxl.styles import Alignment # Needed for Excel text wrapping style

# --- 1. Define File Paths ---
json_file_path = 'IHHP.json'
# Change the output file extension to .xlsx
excel_file_path = 'IHHP_extracts.xlsx'

# --- 2. Load the JSON Data ---
try:
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
except Exception as e:
    print(f"Error loading JSON file: {e}")
    exit()

entities_list = data.get("entities", [])
if not entities_list:
    print("Error: 'entities' key not found or is empty in the JSON data.")
    exit()

# --- 3. Normalize and Flatten the Data (Using iterative method for better column control) ---

all_records = []

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
    # Replace \r\n or \r with simple \n
    return re.sub(r'\r\n|\r', '\n', str(text)) # Note: Need to import 're' at the top if not already there

def create_record(entity, prop1=None, prop2=None):
    return {
        'entity_id': entity.get('id'),
        'entity_type': entity.get('type'),
        'entity_confidence': entity.get('confidence'),
        'entity_mentionText': clean_text(entity.get('mentionText')),
        
        'prop1_id': prop1.get('id') if prop1 else None,
        'prop1_type': prop1.get('type') if prop1 else None,
        'prop1_confidence': prop1.get('confidence') if prop1 else None,
        'prop1_mentionText': clean_text(prop1.get('mentionText')) if prop1 else None,
        
        'prop2_id': prop2.get('id') if prop2 else None,
        'prop2_type': prop2.get('type') if prop2 else None,
        'prop2_confidence': prop2.get('confidence') if prop2 else None,
        'prop2_mentionText': clean_text(prop2.get('mentionText')) if prop2 else None,
    }

# Process the nested JSON data iteratively
for entity in entities_list:
    if entity.get('properties'):
        for prop1 in entity['properties']:
            if prop1.get('properties'):
                for prop2 in prop1['properties']:
                    all_records.append(create_record(entity, prop1, prop2))
            else:
                all_records.append(create_record(entity, prop1))
    else:
        all_records.append(create_record(entity))


df_final = pd.DataFrame(all_records).replace({None: np.nan})

# Clean newlines in text columns and define the target columns for wrapping
text_cols = ['entity_mentionText', 'prop1_mentionText', 'prop2_mentionText']
for col in text_cols:
    if col in df_final.columns:
        df_final[col] = df_final[col].apply(normalize_newlines)

# --- 4. Write to XLSX with Formatting ---

output = io.BytesIO()

# Use 'openpyxl' engine
with pd.ExcelWriter(output, engine='openpyxl') as writer:
    df_final.to_excel(writer, index=False, sheet_name='Data')
    
    # Get the worksheet object
    worksheet = writer.sheets['Data']
    
    # Define the text wrapping alignment style
    wrap_alignment = Alignment(wrap_text=True, vertical='top')
    
    # 1. Map column names to their indices in the final dataframe
    cols_to_wrap = [col for col in text_cols if col in df_final.columns]
    col_indices_to_wrap = [
        df_final.columns.get_loc(col_name) + 1 
        for col_name in cols_to_wrap
    ]

    # 2. Iterate through all rows in the worksheet and apply alignment to relevant cells
    for row in worksheet.iter_rows():
        for cell in row:
            if cell.column in col_indices_to_wrap:
                cell.alignment = wrap_alignment

# Save the BytesIO buffer content to a local file
try:
    with open(excel_file_path, 'wb') as f:
        f.write(output.getvalue())
except Exception as e:
    print(f"Error saving Excel file: {e}")
    exit()


print(f"\nSuccessfully converted JSON to XLSX.")
print(f"Output saved to: {os.path.abspath(excel_file_path)}")
print(f"Total rows generated: {len(df_final)}")
