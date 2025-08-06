import os
import json
import sqlparse
import re

# Global variable for parsing JSON
current_file = None

def parse_sql_schema(sql_text):
    schema = {}
    statements = sqlparse.parse(sql_text)

    for stmt in statements:
        stmt = str(stmt).strip()
        if not stmt.lower().startswith("create table"):
            continue

        # Extract table name
        table_match = re.search(r'create table\s+(\w+)\s*\(', stmt, re.IGNORECASE)
        if not table_match:
            continue

        table_name = table_match.group(1)
        schema[table_name] = {
            "columns": {},
            "primary_key": None,
            "foreign_keys": [],
            "row_count": 10
        }

        # Extract column definitions (excluding constraints)
        column_defs = re.findall(r'(\w+)\s+([\w()]+)[,\n]', stmt)
        for col_name, col_type in column_defs:
            schema[table_name]["columns"][col_name] = {"type": col_type}

        # Extract primary key
        pk_match = re.search(r'primary key\s*\((.*?)\)', stmt, re.IGNORECASE)
        if pk_match:
            pk_column = pk_match.group(1).strip()
            schema[table_name]["primary_key"] = pk_column
            if pk_column in schema[table_name]["columns"]:
                schema[table_name]["columns"][pk_column]["primary_key"] = True

        # Extract foreign keys
        fk_matches = re.findall(
            r'foreign key\s*\((.*?)\)\s*references\s+(\w+)\s*\((.*?)\)', stmt, re.IGNORECASE
        )
        for fk_col, ref_table, ref_col in fk_matches:
            fk_info = {
                "column": fk_col.strip(),
                "ref_table": ref_table.strip(),
                "ref_column": ref_col.strip()
            }
            schema[table_name]["foreign_keys"].append(fk_info)
            if fk_col.strip() in schema[table_name]["columns"]:
                schema[table_name]["columns"][fk_col.strip()]["foreign_key"] = {
                    "table": ref_table.strip(),
                    "column": ref_col.strip()
                }

    return schema

def parse_json_schema(json_text):
    global current_file
    raw = json.loads(json_text)
    schema = {}

    # Get table name from file
    table_name = os.path.splitext(os.path.basename(current_file))[0]
    schema[table_name] = {
        "columns": {},
        "primary_key": raw.get("primaryKey", None),
        "foreign_keys": [],
        "row_count": 10
    }

    for field in raw.get("fields", []):
        col_name = field["name"]
        col_type = field.get("type", "string")

        schema[table_name]["columns"][col_name] = {"type": col_type}

        # Mark as primary key if applicable
        if col_name == raw.get("primaryKey"):
            schema[table_name]["columns"][col_name]["primary_key"] = True

    return schema

def analyze_schema_file(file_path):
    global current_file
    current_file = file_path

    with open(file_path, 'r') as f:
        content = f.read()

    if file_path.endswith(".sql"):
        return parse_sql_schema(content)
    elif file_path.endswith(".json"):
        return parse_json_schema(content)
    else:
        raise ValueError("Unsupported file type: must be .sql or .json")

def analyze_schema(schema_filename):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.abspath(os.path.join(base_dir, "..", "..", "data", "schemas", schema_filename))

    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    return analyze_schema_file(schema_path)

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    schema_dir = os.path.abspath(os.path.join(base_dir, "..", "..", "data", "schemas"))

    if not os.path.exists(schema_dir):
        print(f"[Error] Schema directory not found: {schema_dir}")
        exit(1)

    print("Available schema files:")
    for fname in os.listdir(schema_dir):
        if fname.endswith(".json") or fname.endswith(".sql"):
            print("-", fname)

    schema_file = input("\nEnter schema filename to analyze: ").strip()
    full_path = os.path.join(schema_dir, schema_file)

    if not os.path.exists(full_path):
        print(f"[Error] File not found: {full_path}")
        exit(1)

    try:
        parsed_schema = analyze_schema_file(full_path)
        print("\n✅ Extracted Schema:")
        print(json.dumps(parsed_schema, indent=2))
    except Exception as e:
        print(f"[Error] Failed to parse schema: {e}")
