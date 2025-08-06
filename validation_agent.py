import pandas as pd
import os
import json
import html
import time
from datetime import datetime
import great_expectations as ge
from great_expectations.data_context import get_context
from typing import Dict, List, Any


def convert_type(sql_type: str) -> str:
    """
    Converts SQL-style types to Python types for GE.
    """
    sql_type = sql_type.lower()
    if sql_type.startswith("int"):
        return "int"
    elif sql_type.startswith("varchar") or sql_type == "string":
        return "str"
    elif sql_type.startswith("float") or sql_type.startswith("double"):
        return "float"
    return "str"


def validate_data(
    df: pd.DataFrame,
    table_name: str,
    expectations: dict,
    reference_data: Dict[str, List[Dict[str, Any]]] = None
) -> dict:
    """
    Validates a single table (DataFrame) using Great Expectations rules.
    """
    context = get_context()
    suite_name = f"{table_name}_suite_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    context.add_expectation_suite(expectation_suite_name=suite_name)
    validator = context.sources.pandas_default.read_dataframe(df)

    start = time.time()
    print(f"\n🔍 Validating table: {table_name}")

    for column, rules in expectations.items():
        if rules.get("not_null"):
            validator.expect_column_values_to_not_be_null(column)
        if rules.get("unique"):
            validator.expect_column_values_to_be_unique(column)
        if "type" in rules:
            validator.expect_column_values_to_be_of_type(column, rules["type"])
        if "in_set" in rules:
            validator.expect_column_values_to_be_in_set(column, rules["in_set"])

        # Foreign key checks
        if "foreign_key" in rules and reference_data:
            ref_table, ref_column = rules["foreign_key"]
            if ref_table in reference_data:
                ref_df = pd.DataFrame(reference_data[ref_table])
                if ref_column in ref_df.columns:
                    valid_values = ref_df[ref_column].tolist()
                    validator.expect_column_values_to_be_in_set(column, valid_values)

    result = validator.validate()

    # Debug Output (Full JSON)
    print(f"\n📋 Full Validation Output for '{table_name}':")
    print(json.dumps(result.to_json_dict(), indent=2))

    # Optional: Print only failed expectations
    print(f"\n❌ Failed Expectations Summary for '{table_name}':")
    for res in result.results:
        if not res.success:
            exp_type = res.expectation_config["expectation_type"]
            column = res.expectation_config["kwargs"].get("column")
            unexpected = res.result.get("unexpected_list", [])
            print(f"  ❌ {exp_type} failed on column '{column}'")
            print(f"     Unexpected: {unexpected[:5]}{'...' if len(unexpected) > 5 else ''}")

    print(f"✅ Validation for '{table_name}' completed in {time.time() - start:.2f}s")
    return result


def validate_all_tables(
    data: Dict[str, List[Dict[str, Any]]],
    schema: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Validates all tables in a dataset based on a schema.
    """
    reference_data = {}
    dataframes = {}
    expectations_by_table = {}

    # Step 1: Convert all tables to DataFrames
    for table_name, rows in data.items():
        df = pd.DataFrame(rows)
        dataframes[table_name] = df
        reference_data[table_name] = rows

    # Step 2: Build expectations from schema
    for table_name, details in schema.items():
        table_rules = {}
        for column, dtype in details.get("columns", {}).items():
            rule = {
                "not_null": True,
                "type": convert_type(dtype)
            }

            # Add unique constraint for primary key
            if details.get("primary_key") == column:
                rule["unique"] = True

            # Add foreign key relationships
            for fk in details.get("foreign_keys", []):
                if fk["column"] == column:
                    rule["foreign_key"] = (fk["ref_table"], fk["ref_column"])

            table_rules[column] = rule

        expectations_by_table[table_name] = table_rules

    # Step 3: Validate each table
    results = {}
    for table_name, df in dataframes.items():
        expectations = expectations_by_table.get(table_name, {})
        result = validate_data(df, table_name, expectations, reference_data)
        results[table_name] = result

    return results
