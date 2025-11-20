import pandas as pd
import json
import os
import numpy as np
# --- 1. Define File Paths ---
json_file_path = 'cleaned_data.json'
csv_file_path = 'BHP_5488.csv'

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

# --- 3. Process Entities and Normalize Recursively ---

all_records = []

for entity in entities_list:
    entity_id = entity.get('id')
    entity_type = entity.get('type')
    entity_conf = entity.get('confidence')
    entity_mt = entity.get('mentionText')

    # Case A: Entity has properties list (Level 1 nesting)
    if 'properties' in entity and entity['properties']:
        for prop1 in entity['properties']:
            prop1_id = prop1.get('id')
            prop1_type = prop1.get('type')
            prop1_conf = prop1.get('confidence')
            prop1_mt = prop1.get('mentionText')

            # Case A.1: Level 1 property has its own nested properties list (Level 2 nesting)
            if 'properties' in prop1 and prop1['properties']:
                for prop2 in prop1['properties']:
                    record = {
                        'entity_id': entity_id,
                        'entity_type': entity_type,
                        'entity_confidence': entity_conf,
                        'entity_mentionText': entity_mt,
                        'prop1_id': prop1_id,
                        'prop1_type': prop1_type,
                        'prop1_confidence': prop1_conf,
                        'prop1_mentionText': prop1_mt,
                        'prop2_id': prop2.get('id'),
                        'prop2_type': prop2.get('type'),
                        'prop2_confidence': prop2.get('confidence'),
                        'prop2_mentionText': prop2.get('mentionText')
                    }
                    all_records.append(record)
            
            # Case A.2: Level 1 property is a simple value (e.g., 'Title', 'ValidFrom')
            else:
                record = {
                    'entity_id': entity_id,
                    'entity_type': entity_type,
                    'entity_confidence': entity_conf,
                    'entity_mentionText': entity_mt,
                    'prop1_id': prop1_id,
                    'prop1_type': prop1_type,
                    'prop1_confidence': prop1_conf,
                    'prop1_mentionText': prop1_mt,
                    'prop2_id': None, 'prop2_type': None, 'prop2_confidence': None, 'prop2_mentionText': None
                }
                all_records.append(record)

    # Case B: Entity is flat (no properties list, e.g., 'Logo1')
    else:
        record = {
            'entity_id': entity_id,
            'entity_type': entity_type,
            'entity_confidence': entity_conf,
            'entity_mentionText': entity_mt,
            'prop1_id': None, 'prop1_type': None, 'prop1_confidence': None, 'prop1_mentionText': None,
            'prop2_id': None, 'prop2_type': None, 'prop2_confidence': None, 'prop2_mentionText': None
        }
        all_records.append(record)

# --- 4. Convert records to DataFrame and Write to CSV ---

df_final = pd.DataFrame(all_records)

# Use numpy nan for clean presentation of missing data
df_final = df_final.replace({None: np.nan})

df_final.to_csv(csv_file_path, index=False, encoding='utf-8')

print(f"\nSuccessfully converted JSON to CSV.")
print(f"Output saved to: {os.path.abspath(csv_file_path)}")
print(f"Total rows generated: {len(df_final)}")