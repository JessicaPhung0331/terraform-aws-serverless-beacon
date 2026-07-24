import re

def extract(raw_input, dataset_id, delimiter="", comment=""):
    unix_text = re.sub(r'\r', '', raw_input)
    lines = unix_text.split('\n')
    keys = []
    extracted = []

    for line in lines:
        if not line or (comment and line.startswith(comment)):
            continue

        cols = line.split(delimiter)

        # Fix column names
        if len(keys) == 0:
            keys = [col.lower().strip().replace(" ", "_").replace("-", "_") for col in cols]
            keys = ["sample_id" if key == "sample_name" else key for key in keys]
            print(f"Found columns: {keys}")

        elif len(keys) == len(cols):
            entry = {}
            entry["dataset_id"] = dataset_id
            entry.update(dict(zip(keys, cols)))
            extracted.append(entry)

        else:
            print("Warning: line may be incorrect")
            print(line)
        
    print("Successfully read file into dict")

    return extracted