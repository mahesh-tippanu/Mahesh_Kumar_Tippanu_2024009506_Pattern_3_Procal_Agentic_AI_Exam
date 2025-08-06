## 📝 Project Description

This project implements an **Agentic AI-based Test Data Generation and Validation System** designed to automate the preparation of high-quality, referentially consistent test datasets for complex relational database schemas. It uses a modular, agent-driven architecture, where each agent is responsible for a specific task in the data preparation pipeline—from schema analysis to synthetic data generation to validation against integrity and business rules.

The system is especially useful for testing large-scale data systems, ETL pipelines, and compliance scenarios where realistic yet synthetic data is required without violating referential integrity.

### 💡 Key Features

- 🔍 **Schema Analysis Agent**  
  Parses SQL or JSON schema definitions to detect tables, primary/foreign key relationships, and data types.

- 🛠️ **Data Generation Agent**  
  Produces synthetic test data with support for referential integrity across parent-child table structures.

- ✅ **Validation Agent**  
  Utilizes [Great Expectations](https://greatexpectations.io/) to enforce data quality checks including:
  - Null value checks
  - Uniqueness
  - Type consistency
  - Foreign key constraints
  - Business logic (e.g., salary > 0, date in range)

- 🔁 **Agentic Orchestration**  
  Fully automated pipeline with extensibility for additional agents (e.g., data masking, anonymization, export).

- 🧪 **Testable & Repeatable**  
  Plug-and-play schema input → repeatable data output → auto-validation → final export.
