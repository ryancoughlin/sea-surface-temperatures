import json

SERVER_URL = 'http://157.245.10.94'

def process_path(path):
    return f"{SERVER_URL}/{path.replace('output/', '')}"

# Read the current metadata
with open('output/metadata.json', 'r') as f:
    data = json.load(f)

# Transform the paths
for item in data:
    item['image'] = process_path(item['image'])

# Write back the updated metadata
with open('output/metadata.json', 'w') as f:
    json.dump(data, f, indent=2) 