import streamlit as st
import os
import sys
import re

# Add parent directory to system path to import agents
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your agent modules
from agents.configuration_agent import configure_generation
from agents.schema_analysis_agent import analyze_schema_file
from agents.data_generation_agent import generate_data
from agents.validation_agent import validate_all_tables

# Streamlit Page Setup
st.set_page_config(page_title="Agentic AI Assistant", layout="centered")
st.title("🤖 Agentic AI Assistant")
st.markdown("Ask me to generate and validate data from any schema! Example: `Generate 100 rows for creditcardfraud-metadata.json as JSON`")

# Chat message history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display previous messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# User input handling
if prompt := st.chat_input("Type your request..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("🤔 Processing your request..."):

            try:
                # ✅ Extract parameters using regex
                match = re.search(r"generate\s+(\d+)\s+rows\s+for\s+([\w\-]+\.json)\s+as\s+(\w+)", prompt, re.IGNORECASE)
                if not match:
                    raise ValueError("❌ Could not parse the request. Please use format like: `Generate 100 rows for creditcardfraud-metadata.json as JSON`")

                row_count = int(match.group(1))
                schema_file = match.group(2)
                export_format = match.group(3).lower()

                # ✅ Step 1: Configuration
                config = configure_generation(
                    data_volume=row_count,
                    export_format=export_format,
                    custom_rules=None,
                    privacy_settings={"GDPR": True}
                )

                # ✅ Step 2: Schema path
                schema_path = os.path.join("data", "schemas", schema_file)
                if not os.path.exists(schema_path):
                    raise FileNotFoundError(f"❌ Schema file not found: {schema_path}")

                # ✅ Step 3: Analyze schema
                parsed_schema = analyze_schema_file(schema_path)

                # ✅ Step 4: Generate data
                generated_data = generate_data(parsed_schema, row_count)

                # ✅ Step 5: Validate data
                validation_results = validate_all_tables(generated_data, parsed_schema)

                # ✅ Step 6: Final response
                success_msg = f"✅ Generated and validated `{row_count}` rows for **{schema_file}** as **{export_format.upper()}**."
                st.session_state.messages.append({"role": "assistant", "content": success_msg})
                st.markdown(success_msg)

            except Exception as e:
                error_msg = f"❌ Error: {str(e)}"
                st.session_state.messages.append({"role": "assistant", "content": error_msg})
                st.markdown(error_msg)
