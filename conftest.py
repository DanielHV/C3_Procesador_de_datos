import json
import os
from dotenv import load_dotenv

load_dotenv()

dict_path = os.getenv("DICTIONARY_CSV_PATH")

if dict_path and os.path.exists(dict_path):
    with open(dict_path) as f:
        config = json.load(f)
        for key, value in config.items():
            os.environ[key] = value if isinstance(value, str) else json.dumps(value)
else:
    print(f"Error: Could not find the file at {dict_path}")
