import sqlite3
import json

def infer_data_type(value):
    """
    Infer the data type of the value (int, float, or text).
    """
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    else:
        return "TEXT"

def create_table(cursor, table_name, columns, sample_data):
    """
    Create a table dynamically based on columns from the JSON data.
    Infer column data types from sample data.
    Handle 'id' as PRIMARY KEY to avoid duplicates.
    """
    # Ensure 'id' is always the first column and not included as part of dynamic columns
    if 'id' in columns:
        columns.remove('id')  # Remove 'id' from dynamic columns list
    
    # Define the column types based on sample data
    column_definitions = []
    for col in columns:
        column_type = infer_data_type(sample_data[0].get(col, None))
        column_definitions.append(f"{col} {column_type}")

    # Define the 'id' column as TEXT (or INTEGER, depending on your preference)
    column_definitions.insert(0, "id TEXT PRIMARY KEY")

    # Create the table with inferred column types
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
    cursor.execute(create_query)

def convert_value(value):
    """
    Convert the value to the appropriate type.
    Handle lists by converting them to a comma-separated string.
    """
    if isinstance(value, list):
        # Convert list to a comma-separated string
        return ', '.join(map(str, value))
    elif isinstance(value, bool):
        # Convert boolean values to 0 (False) or 1 (True)
        return 1 if value else 0
    elif isinstance(value, int):
        return value
    elif isinstance(value, float):
        return value
    else:
        # Default to string type for other cases (e.g., None becomes 'NULL' if necessary)
        return str(value) if value is not None else None

def insert_data(cursor, table_name, columns, data):
    """
    Insert data into the table dynamically based on the columns from JSON.
    """
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["?" for _ in columns])

    # Insert each row of data into the table
    for row in data:
        values = []
        for col in columns:
            value = row.get(col, None)
            values.append(convert_value(value))  # Convert value to the appropriate type

        # Insert the row into the database
        cursor.execute(f"INSERT OR REPLACE INTO {table_name} ({columns_str}) VALUES ({placeholders})", values)

def process_json_to_db(json_file, db_file):
    """
    Read the JSON file and populate the database dynamically.
    """
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Process each top-level key in the JSON (which will correspond to table names)
    for table_name, table_data in data.items():
        if isinstance(table_data, list) and len(table_data) > 0:
            # Assume all elements in the list have the same structure (same keys)
            columns = table_data[0].keys()  # Extract columns from the first item
            create_table(cursor, table_name, list(columns), table_data)  # Create the table with the identified columns
            insert_data(cursor, table_name, columns, table_data)  # Insert data into the table
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    print("Data inserted into the database successfully!")

# Example usage
process_json_to_db('./new_data/data.json', './new_data/employee_database.db')