import pandas_monday as pm

client = pm.monday_pandas()
BOARD_ID = 6225157636


def read_board_example():
    df = client.read_board(
        board_id=BOARD_ID,
        include_subitems=True,
    )
    return df


if __name__ == "__main__":
    print(read_board_example().columns)
