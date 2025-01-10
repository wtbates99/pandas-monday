import pandas_monday as pm
import pandas as pd

client = pm.monday_pandas(
    api_token="eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjQ0OTEyMDg1MCwiYWFpIjoxMSwidWlkIjo1MzY5NjA4NSwiaWFkIjoiMjAyNC0xMi0xN1QxNDozNDoxMy4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MzM3OTc2MywicmduIjoidXNlMSJ9.vmSuFA8cF-5hq6Wiwb-b6yF6f1gbkBCZZNQR7o36K7c"
)
BOARD_ID = 5781809869


def read_board_example():
    df = client.read_board(
        board_id=BOARD_ID,
        include_subitems=True,
    )
    return df


def write_board_example():
    # Example DataFrame with columns that match your Monday.com board
    data = {
        "Description": ["Task 1", "Task 2", "Task 3"],
        "Status": ["Done", "In Progress", "Todo"],
        "Priority": ["High", "Medium", "Low"],
        "Assignee": ["", "", ""],  # Empty strings for optional fields
        "Due date": ["", "", ""],
        "Time Sink": ["", "", ""],
        "Department": ["", "", ""],
        "Files": ["", "", ""],
        "Google Calendar event": ["", "", ""],
    }
    df = pd.DataFrame(data)

    # Write the DataFrame to the board
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
