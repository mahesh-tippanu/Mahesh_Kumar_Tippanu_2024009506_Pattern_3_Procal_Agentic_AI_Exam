from flask import Flask, render_template, request, jsonify
import os
import re
import time

from agents.configuration_agent import configure_generation, export_data
from agents.schema_analysis_agent import analyze_schema_file
from agents.data_generation_agent import generate_data
from agents.validation_agent import validate_all_tables

app = Flask(__name__, template_folder="templates", static_folder="static")

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/chat", methods=["POST"])
def chat():
    user_message = request.json.get("message", "").strip()

    if not user_message:
        return jsonify({"response": "❌ Please enter a valid message."})

    try:
        # Expecting input format like:
        # "Generate 100 rows for ecommerce-data-metadata.json as json"
        pattern = r"generate\s+(\d+)\s+rows\s+for\s+(\S+)\s+as\s+(\w+)"
        match = re.search(pattern, user_message.lower())

        if not match:
            return jsonify({"response": "❗ Please use format: Generate <rows> rows for <schema>.json as <format>"})

        row_count = int(match.group(1))
        schema_file = match.group(2)
        export_format = match.group(3)

        # Step 1: Configure
        config = configure_generation(
            data_volume=row_count,
            export_format=export_format,
            custom_rules=None,
            privacy_settings={"GDPR": True}
        )

        # Step 2: Locate Schema
        schema_path = os.path.join("data", "schemas", schema_file)
        if not os.path.exists(schema_path):
            return jsonify({"response": f"❌ Schema file not found: {schema_file}"})

        # Step 3: Analyze Schema
        parsed_schema = analyze_schema_file(schema_path)

        # Step 4: Generate Data
        start = time.time()
        generated_data = generate_data(parsed_schema, config["data_volume"])
        duration = time.time() - start

        # Step 5: Validate
        validate_all_tables(generated_data, parsed_schema)

        # Step 6: Export
        output_file = export_data(generated_data, config)

        return jsonify({
            "response": (
                f"✅ Generated {row_count} rows from `{schema_file}` as `{export_format}` in {duration:.2f}s.\n"
                f"📁 Data exported to: `{output_file}`"
            )
        })

    except Exception as e:
        return jsonify({"response": f"❌ Error: {str(e)}"})


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
