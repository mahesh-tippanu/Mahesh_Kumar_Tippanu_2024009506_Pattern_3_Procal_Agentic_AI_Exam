import json
import os
import pandas as pd
import html
from typing import Dict, List, Any


def configure_generation(
    data_volume: int,
    custom_rules: dict = None,
    privacy_settings: dict = None,
    export_format: str = 'CSV'
) -> dict:
    """
    Configures the data generation process based on user-defined requirements.
    """
    supported_formats = ['CSV', 'JSON', 'SQL', 'XML']
    formatted_export_format = export_format.upper()

    if formatted_export_format not in supported_formats:
        raise ValueError(f"Unsupported export format: '{export_format}'. Supported formats are: {', '.join(supported_formats)}")

    config = {
        'data_volume': data_volume,
        'custom_rules': custom_rules if custom_rules else {},
        'privacy_settings': privacy_settings if privacy_settings else {},
        'export_format': formatted_export_format
    }

    return config


def export_data(
    data: Dict[str, List[Dict[str, Any]]],
    config: dict,
    output_dir: str = 'data/outputs'
) -> None:
    """
    Exports the generated data to the specified format.
    """
    format = config['export_format']
    os.makedirs(output_dir, exist_ok=True)

    print(f"\n📤 Exporting data in {format} format to '{output_dir}'")

    for table_name, table_data in data.items():
        df = pd.DataFrame(table_data)
        file_path = os.path.join(output_dir, f"{table_name}_generated.{format.lower()}")

        if format == 'CSV':
            df.to_csv(file_path, index=False)
            print(f"  ✅ CSV: {table_name} exported to {file_path}")

        elif format == 'JSON':
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(table_data, f, indent=4)
            print(f"  ✅ JSON: {table_name} exported to {file_path}")

        elif format == 'SQL':
            with open(file_path, 'w', encoding='utf-8') as f:
                for row in table_data:
                    columns = ', '.join(row.keys())
                    values = ', '.join(
                        f"'{str(v).replace('\'', '\'\'')}'" if isinstance(v, str) else str(v)
                        for v in row.values()
                    )
                    f.write(f"INSERT INTO {table_name} ({columns}) VALUES ({values});\n")
            print(f"  ✅ SQL: {table_name} export with INSERTs to {file_path}")

        elif format == 'XML':
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"<data><table name='{table_name}'>\n")
                for row in table_data:
                    f.write(f"  <row>\n")
                    for k, v in row.items():
                        f.write(f"    <{k}>{html.escape(str(v))}</{k}>\n")
                    f.write(f"  </row>\n")
                f.write(f"</table></data>\n")
            print(f"  ✅ XML: {table_name} exported to {file_path}")

        else:
            print(f"❌ Error: Unsupported export format '{format}' for {table_name}.")


# ----------------------------
# 🔍 Self-Test Block
# ----------------------------
if __name__ == '__main__':
    output_test_dir = 'test_outputs'
    os.makedirs(output_test_dir, exist_ok=True)

    try:
        # Test Configuration
        user_config_csv = configure_generation(
            data_volume=5,
            privacy_settings={'gdpr_masking': True},
            export_format='CSV'
        )

        user_config_json = configure_generation(
            data_volume=5,
            custom_rules={'age_range': (18, 65)},
            export_format='JSON'
        )

        sql_config = configure_generation(
            data_volume=2,
            export_format='SQL'
        )

        xml_config = configure_generation(
            data_volume=2,
            export_format='XML'
        )

    except ValueError as e:
        print(f"\n❌ Configuration Error: {e}")
        exit(1)

    # Sample data
    sample_data_to_export = {
        'users': [
            {'id': 1, 'name': 'Alice & Bob', 'email': 'alice@example.com'},
            {'id': 2, 'name': 'Charlie <Admin>', 'email': 'charlie@domain.org'}
        ],
        'products': [
            {'prod_id': 101, 'name': 'Laptop "Pro"', 'price': 1200},
            {'prod_id': 102, 'name': 'Mouse & Keyboard', 'price': 150}
        ]
    }

    # Run Exports
    print("\n--- Running Export Tests ---")
    export_data(sample_data_to_export, user_config_csv, output_test_dir)
    export_data(sample_data_to_export, user_config_json, output_test_dir)
    export_data(sample_data_to_export, sql_config, output_test_dir)
    export_data(sample_data_to_export, xml_config, output_test_dir)

    print(f"\n✅ All files generated successfully in '{output_test_dir}' directory.")
