import pandas_monday as pm
import pandas as pd

client = pm.monday_pandas()
BOARD_ID = 5781809869


def read_board_example():
    df = client.read_board(
        board_id=BOARD_ID,
        include_subitems=True,
    )
    return df


# Update your example.py with better error handling
def write_board_example():
    data = {
        "Description": ["Task 1", "Task 2", "Task 3"],
        "Status": ["Done", "Working on it", "TBD"],
        "Priority": ["High", "Medium", "Low"],
        "Assignee": ["", "", ""],
        "Due date": ["", "", ""],
        "Time Sink": ["", "", ""],
        "Department": ["", "", ""],
        "Files": ["", "", ""],
        "Google Calendar event": ["", "", ""],
    }
    df = pd.DataFrame(data)

    client.write_board(
        board_id=BOARD_ID,
        df=df,
        mode="replace",
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
