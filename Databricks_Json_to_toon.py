from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, to_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import json
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DeltaTOONStorage") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ============================================
# TOON Converter Functions
# ============================================

def json_to_toon(json_dict):
    """Convert JSON dict to TOON format string."""
    import re
    
    toon_output = []
    
    for key, value in json_dict.items():
        if isinstance(value, list) and len(value) > 0:
            if all(isinstance(item, dict) for item in value):
                # Handle list of dicts (tabular data)
                keys = list(value[0].keys())
                key_str = ",".join(keys)
                count = len(value)
                header = f"{key}[{count}]{{{key_str}}}:"
                toon_output.append(header)
                
                for record in value:
                    values = [str(record.get(k, "")) for k in keys]
                    toon_output.append(",".join(values))
            else:
                toon_output.append(f"{key}:{','.join(str(v) for v in value)}")
        elif isinstance(value, dict):
            toon_output.append(f"{key}:{{")
            for k, v in value.items():
                toon_output.append(f"  {k}:{v}")
            toon_output.append("}")
        else:
            toon_output.append(f"{key}:{value}")
    
    return "\n".join(toon_output)


def toon_to_json(toon_str):
    """Convert TOON format string back to JSON dict."""
    import re
    
    lines = toon_str.strip().split("\n")
    result = {}
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        if not line:
            i += 1
            continue
        
        # Parse table format: key[count]{fields}:
        table_match = re.match(r"^(\w+)\[(\d+)\]\{([^}]+)\}:", line)
        if table_match:
            key, count, fields = table_match.groups()
            field_list = [f.strip() for f in fields.split(",")]
            rows = []
            
            for j in range(int(count)):
                i += 1
                if i < len(lines):
                    data_line = lines[i].strip()
                    values = data_line.split(",")
                    record = {field_list[k]: _cast_value(values[k]) 
                             for k in range(len(field_list))}
                    rows.append(record)
            
            result[key] = rows
        
        elif ":" in line:
            parts = line.split(":", 1)
            if len(parts) == 2:
                key, value = parts
                key = key.strip()
                value = value.strip()
                result[key] = _cast_value(value)
        
        i += 1
    
    return result


def _cast_value(value):
    """Cast string value to appropriate type."""
    value = value.strip()
    if value.lower() == "true":
        return True
    elif value.lower() == "false":
        return False
    elif value.isdigit():
        return int(value)
    else:
        try:
            return float(value)
        except ValueError:
            return value


# ============================================
# Create Delta Table
# ============================================

# Drop table if exists
spark.sql("DROP TABLE IF EXISTS default.data_storage")

# Create Delta table schema
table_schema = StructType([
    StructField("id", StringType(), False),
    StructField("json_format", StringType(), True),
    StructField("toon_format", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Create table
spark.sql("""
    CREATE TABLE IF NOT EXISTS default.data_storage (
        id STRING NOT NULL,
        json_format STRING,
        toon_format STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
""")

print("✅ Delta table 'default.data_storage' created")

# ============================================
# Insert Sample Data
# ============================================

sample_data = {
    "users": [
        {"id": 1, "name": "Alice", "role": "admin", "score": 95},
        {"id": 2, "name": "Bob", "role": "user", "score": 87},
        {"id": 3, "name": "Charlie", "role": "editor", "score": 91}
    ]
}

# Convert to TOON
toon_data = json_to_toon(sample_data)
json_data = json.dumps(sample_data)

# Create PySpark DataFrame and insert
from datetime import datetime

data_to_insert = [
    {
        "id": "users_001",
        "json_format": json_data,
        "toon_format": toon_data,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }
]

df_insert = spark.createDataFrame(data_to_insert, schema=table_schema)
df_insert.write.mode("append").format("delta").mode("append").saveAsTable("default.data_storage")

print("✅ Sample data inserted into Delta table")

# ============================================
# Read and Display
# ============================================

print("\n📊 Delta Table Content:")
spark.sql("SELECT * FROM default.data_storage").show(truncate=False)

# ============================================
# Insert More Data Using SQL
# ============================================

products_data = {
    "products": [
        {"id": 101, "name": "Laptop", "price": 999.99},
        {"id": 102, "name": "Mouse", "price": 29.99}
    ]
}

products_toon = json_to_toon(products_data)
products_json = json.dumps(products_data)

spark.sql(f"""
    INSERT INTO default.data_storage
    VALUES (
        'products_001',
        '{products_json.replace("'", "\\'")}',
        '{products_toon.replace("'", "\\'")}',
        current_timestamp(),
        current_timestamp()
    )
""")

print("\n✅ Additional data inserted")

# ============================================
# Query Examples
# ============================================

print("\n📋 All Records:")
spark.sql("SELECT id FROM default.data_storage").show()

print("\n🔍 View JSON Format:")
spark.sql("SELECT id, json_format FROM default.data_storage WHERE id = 'users_001'").show(truncate=False)

print("\n🔍 View TOON Format:")
spark.sql("SELECT id, toon_format FROM default.data_storage WHERE id = 'users_001'").show(truncate=False)

# ============================================
# Verify Conversion (Round-trip)
# ============================================

print("\n✅ Conversion Verification:")
result = spark.sql("SELECT json_format, toon_format FROM default.data_storage WHERE id = 'users_001' LIMIT 1").collect()

if result:
    json_str = result[0]["json_format"]
    toon_str = result[0]["toon_format"]
    
    # Convert TOON back to JSON
    reconstructed = toon_to_json(toon_str)
    original = json.loads(json_str)
    
    print(f"Original JSON keys: {list(original.keys())}")
    print(f"Reconstructed from TOON keys: {list(reconstructed.keys())}")
    print(f"Match: {original == reconstructed} ✅" if original == reconstructed else f"Match: False ❌")

# ============================================
# Helper Functions for Application Usage
# ============================================

def insert_data_to_delta(record_id, data_dict):
    """Insert JSON data to Delta table (automatically converts to TOON)."""
    json_str = json.dumps(data_dict)
    toon_str = json_to_toon(data_dict)
    
    spark.sql(f"""
        INSERT INTO default.data_storage
        VALUES (
            '{record_id}',
            '{json_str.replace("'", "\\'")}',
            '{toon_str.replace("'", "\\'")}',
            current_timestamp(),
            current_timestamp()
        )
    """)
    print(f"✅ Data stored with ID: {record_id}")


def retrieve_as_json(record_id):
    """Retrieve data as JSON."""
    result = spark.sql(f"SELECT json_format FROM default.data_storage WHERE id = '{record_id}'").collect()
    if result:
        return json.loads(result[0]["json_format"])
    return None


def retrieve_as_toon(record_id):
    """Retrieve data as TOON."""
    result = spark.sql(f"SELECT toon_format FROM default.data_storage WHERE id = '{record_id}'").collect()
    if result:
        return result[0]["toon_format"]
    return None


# Example usage:
# insert_data_to_delta("order_001", {"orders": [{"id": 1, "amount": 100}]})
# json_data = retrieve_as_json("order_001")
# toon_data = retrieve_as_toon("order_001")
