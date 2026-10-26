"""
Read a demo input file and print the union config it holds, as compact JSON.

    python3 demo/read_input.py demo/input.json

The file can be any of:
  • a bare config:            {"snap": {...}, "dedup": {...}, "connect": {...}, "extensions": {...}}
  • a wrapper with "config":  {"about": "...", "config": {...}}   (what demo/input.json ships as)
  • a whole job request:      {"tdei_dataset_id_one": "...", "tdei_dataset_id_two": "...", "config": {...}}
                              (dataset ids are ignored: the demo always runs on its own two datasets)
"config" may also be a JSON string, as the TDEI API accepts. The union itself
checks the config's contents; this only finds it. Exits non-zero with a message
when the file is not valid JSON or holds no config.
"""
import json, sys

SECTIONS = {"snap", "dedup", "connect", "extensions"}

def find_config(obj, depth=0):
    if not isinstance(obj, dict):
        return None
    if "config" in obj:
        c = obj["config"]
        if isinstance(c, str):
            c = json.loads(c) if c.strip() else {}
        return {} if c is None else c
    if not obj or SECTIONS & set(obj):
        return obj                                   # a bare config ({} = nothing configured)
    if depth < 3:
        for v in obj.values():
            found = find_config(v, depth + 1)
            if found is not None:
                return found
    return None

path = sys.argv[1]
try:
    data = json.load(open(path, encoding="utf-8"))
except json.JSONDecodeError as e:
    sys.exit(f"{path} is not valid JSON: {e.msg} at line {e.lineno}, column {e.colno}")
except OSError as e:
    sys.exit(f"cannot read {path}: {e.strerror}")
try:
    config = find_config(data)
except json.JSONDecodeError as e:
    sys.exit(f"{path}: \"config\" is a string but not valid JSON: {e.msg}")
if config is None:
    sys.exit(f"{path} holds no union config: expected snap / dedup / connect / extensions sections, "
             f"or an object with a \"config\" key")
if not isinstance(config, dict):
    sys.exit(f"{path}: the config must be a JSON object (got {type(config).__name__})")
print(json.dumps(config, separators=(",", ":")))
