import pandas_monday as pm


# Create an instance of monday_pandas
client = pm.monday_pandas()

# Test reading a board
try:
    df = client.read_board(
        board_id=6225157636,
        progress_bar=True,  # Optional: shows progress while fetching
        include_subitems=False,  # Optional: include subitems if any
    )

    # Print basic information about the retrieved data
    print("\nDataFrame Info:")
    print(df.info())

    print("\nFirst few rows:")
    print(df.head())

except Exception as e:
    print(f"Error: {str(e)}")
