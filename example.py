import pandas as pd

import pandas_monday as pm

client = pm.monday_pandas()

BOARD_ID = 8207868466


def read_board_example() -> pd.DataFrame:
    df = client.read_board(
        board_id=BOARD_ID,
        include_subitems=True,
    )
    return df


def write_board_example() -> None:
    df = pd.read_csv("data.csv")

    client.write_board(
        board_id=BOARD_ID,
        df=df,
        mode="replace",
        overwrite_type="archive",
    )


if __name__ == "__main__":
    df = read_board_example()
    write_board_example()
