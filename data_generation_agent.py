import os
import random
from faker import Faker

fake = Faker()

def generate_data(parsed_schema, config):
    generated_ids = {}
    generated_data = {}

    def generate_table_data(table_name, schema):
        table_rows = []
        primary_key_column = None

        for _ in range(schema["row_count"]):
            row = {}
            for column, properties in schema["columns"].items():
                if properties.get("primary_key"):
                    value = len(table_rows) + 1
                    primary_key_column = column
                elif "foreign_key" in properties:
                    ref_table = properties["foreign_key"]["table"]
                    ref_column = properties["foreign_key"]["column"]
                    value = random.choice(generated_ids[ref_table])
                else:
                    if properties["type"] == "string":
                        value = fake.name() if "name" in column else fake.email()
                    elif properties["type"] == "int":
                        value = random.randint(100, 1000)
                    else:
                        value = None
                row[column] = value

            table_rows.append(row)

        # Store primary keys
        if primary_key_column:
            generated_ids[table_name] = [row[primary_key_column] for row in table_rows]

        generated_data[table_name] = table_rows

    # Step 1: Generate parent tables
    for table, schema in parsed_schema.items():
        has_fk = any("foreign_key" in col for col in schema["columns"].values())
        if not has_fk:
            generate_table_data(table, schema)

    # Step 2: Generate child tables
    for table, schema in parsed_schema.items():
        has_fk = any("foreign_key" in col for col in schema["columns"].values())
        if has_fk:
            generate_table_data(table, schema)

    return generated_data


def generate_data(parsed_schema, row_count):
    table_name, table_info = next(iter(parsed_schema.items()))
    columns = table_info.get("columns", {})
    data = []

    for _ in range(row_count):
        row = {}
        for col, props in columns.items():
            dtype = props.get("type", "string").lower()
            if dtype == "int":
                row[col] = random.randint(1, 1000)
            elif dtype == "float":
                row[col] = round(random.uniform(1, 1000), 2)
            elif dtype == "bool":
                row[col] = random.choice([True, False])
            else:
                row[col] = f"{col}_{random.randint(1000, 9999)}"
        data.append(row)
    
    return {table_name: data}
