import pandas_monday as pm
import pandas as pd

client = pm.monday_pandas()

BOARD_ID = 8207868466


def read_board_example():
    df = client.read_board(
        board_id=BOARD_ID,
        include_subitems=True,
    )
    return df


# Update your example.py with better error handling
def write_board_example():
    df = pd.read_csv("data.csv")

    client.write_board(
        board_id=BOARD_ID,
        df=df,
        mode="replace",
        overwrite_type="archive",
    )
    print("DataFrame successfully written to the board.")


if __name__ == "__main__":
    # Read the board
    print("Reading board...")
    df = read_board_example()
    print(df.columns)

    # Write to the board
    print("Writing to board...")
    write_board_example()
