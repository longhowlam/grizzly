import grizzly

# Read the SAS7BDAT file
print("Reading airline.sas7bdat...")
df = grizzly.read_sas("airline.sas7bdat")

# Display basic info
print(f"Rows: {df.row_count()}, Columns: {df.column_count()}")
print(f"Shape: {df.shape}")

# Display the data
print("\nData preview:")
df.show()
