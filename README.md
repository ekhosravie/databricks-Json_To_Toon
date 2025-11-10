 TOON Delta Storage - Token-Efficient Data Format for Databricks
Store and retrieve data in both JSON and TOON (Token-Oriented Object Notation) formats simultaneously in a Databricks Delta table. Automatically convert between formats while maintaining data integrity.
 What is TOON?
TOON is a lightweight, token-efficient alternative to JSON designed specifically for LLM applications. It reduces payload size by 30-60% by eliminating unnecessary punctuation and using tabular patterns with indentation.
JSON:
json{
  "users": [
    { "id": 1, "name": "Alice" },
    { "id": 2, "name": "Bob" }
  ]
}
TOON:
users[2]{id,name}:
1,Alice
2,Bob
Same data. Same meaning. Roughly half the tokens. 
 Features
 Automatic JSON ↔ TOON conversion
 Delta table storage with timestamps
 Round-trip verification (convert TOON back to JSON with 100% accuracy)
 Helper functions for easy insert/retrieve operations
 Sample data and queries included
 Works seamlessly with Databricks default schema

 Table Schema
id STRING (NOT NULL)          - Unique record identifier
json_format STRING            - Original data in JSON format
toon_format STRING            - Same data in TOON format
created_at TIMESTAMP          - Record creation time
updated_at TIMESTAMP          - Record last update time


pythonfrom pyspark.sql import SparkSession

# Initialize
spark = SparkSession.builder.appName("DeltaTOONStorage").getOrCreate()

# Run the script to create table and insert sample data
exec(open("delta_toon_storage.py").read())

# Insert new data
insert_data_to_delta("order_001", {
    "orders": [
        {"id": 1, "product": "Laptop", "price": 999.99},
        {"id": 2, "product": "Mouse", "price": 29.99}
    ]
})

# Retrieve as JSON
json_data = retrieve_as_json("order_001")
print(json_data)

# Retrieve as TOON
toon_data = retrieve_as_toon("order_001")
print(toon_data)
 Key Functions
insert_data_to_delta(record_id, data_dict)

Inserts a Python dictionary as both JSON and TOON formats
Automatically timestamps the record

retrieve_as_json(record_id)

Retrieves stored data in JSON format
Returns parsed Python dictionary

retrieve_as_toon(record_id)

Retrieves stored data in TOON format
Returns TOON string representation

json_to_toon(json_dict)

Core conversion function
Supports flat structures, lists, and nested objects

toon_to_json(toon_str)

Reverse conversion function
Maintains data integrity and type casting

 Use Cases
LLM Applications: Store prompt templates and structured outputs in TOON to reduce token consumption
API Responses: Cache responses in both formats for flexibility
Data Pipelines: Convert data formats on-the-fly without separate transformations
Cost Optimization: Reduce storage and API costs by using TOON for LLM interactions
Data Comparison: Track how different formats represent the same information
 JSON vs TOON
FeatureJSONTOONVerbosityHighLowToken UsageHigh30-60% fewerReadabilityModerateHighNesting SupportExcellent LimitedBest ForAPIs, universal storageLLM prompts, flat data
 When to Use TOON
 Best suited for:

Flat, tabular data (user lists, product catalogs)
LLM prompts and structured outputs
Dataset rows with consistent schema

 Avoid for:

Deeply nested hierarchies
Complex relational structures
Data requiring maximum compatibility

 Verification Example
The script automatically verifies conversion accuracy:
 Conversion Verification:
Original JSON keys: ['users']
Reconstructed from TOON keys: ['users']
Match: True 
📦 Requirements
pyspark >= 3.0
delta >= 1.0
python >= 3.8
🛠️ Installation

Clone this repository
Run in a Databricks notebook or Spark environment
The script automatically creates the Delta table in the default schema

 Example Output
 Delta table 'default.data_storage' created
 Sample data inserted into Delta table

 Delta Table Content:
+----------+------------------+------------------+-----+-----+
|id        |json_format       |toon_format       |...  |...  |
+----------+------------------+------------------+-----+-----+
|users_001 |{"users":[{"i...  |users[3]{id,n...  |...  |...  |
+----------+------------------+------------------+-----+-----+

TOON Format Specification
Databricks Delta Lake Documentation
Token Efficiency in LLMs
