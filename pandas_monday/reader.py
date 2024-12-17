from typing import Dict, List, Optional, Union, Any
import pandas as pd
import tqdm
from . import exceptions


def read_board(
    executor,  # This will be the monday_pandas instance
    board_id: Union[str, int],
    columns: Optional[List[str]] = None,
    filter_criteria: Optional[Dict[str, Any]] = None,
    max_results: Optional[int] = None,
    progress_bar: bool = True,
    include_subitems: bool = False,
    page_size: int = 100,
) -> pd.DataFrame:
    """Read a board from Monday.com and return it as a DataFrame."""
    meta_query = """
    query ($board_id: ID!) {
        boards(ids: [$board_id]) {
            name
            columns { id title }
        }
    }
    """
    meta_response = executor._execute_query(meta_query, {"board_id": str(board_id)})

    if not meta_response.get("data", {}).get("boards"):
        raise exceptions.monday_pandas_board_not_found_error(
            f"Board {board_id} not found"
        )

    board = meta_response["data"]["boards"][0]
    column_mapping = {col["id"]: col["title"] for col in board["columns"]}

    items_query = """
    query ($board_id: ID!, $cursor: String, $page_size: Int!) {
        boards(ids: [$board_id]) {
            items_page(limit: $page_size, cursor: $cursor) {
                cursor
                items {
                    id
                    name
                    group { title }
                    column_values { id text }
                    subitems {
                        id
                        name
                        column_values { id text }
                    }
                }
            }
        }
    }
    """

    records = []
    cursor = None
    total_items = 0

    with tqdm.tqdm(disable=not (progress_bar and tqdm)) as pbar:
        while True:
            variables = {
                "board_id": str(board_id),
                "cursor": cursor,
                "page_size": page_size,
            }

            response = executor._execute_query(items_query, variables)
            items_page = response["data"]["boards"][0]["items_page"]
            items = items_page["items"]

            for item in items:
                record = {
                    "board_id": item["id"],
                    "board_name": board["name"],
                    "group": item["group"]["title"],
                    "board_item": item["name"],
                    "is_subitem": False,
                    "subitem_text": None,
                }

                for col in item["column_values"]:
                    col_title = column_mapping.get(col["id"], col["id"])
                    record[col_title] = col["text"]

                records.append(record)

                if include_subitems and item.get("subitems"):
                    for subitem in item["subitems"]:
                        subitem_record = {
                            "board_id": subitem["id"],
                            "board_name": board["name"],
                            "group": item["group"]["title"],
                            "board_item": item["name"],
                            "is_subitem": True,
                            "subitem_text": subitem["name"],
                        }

                        for col in subitem["column_values"]:
                            col_title = column_mapping.get(col["id"], col["id"])
                            subitem_record[col_title] = col["text"]

                        records.append(subitem_record)

            total_items += len(items)
            pbar.update(len(items))

            if max_results and total_items >= max_results:
                records = records[:max_results]
                break

            cursor = items_page.get("cursor")
            if not cursor or not items:
                break

    df = pd.DataFrame.from_records(records)
    df = df.drop(columns=["Subitems"])

    if not include_subitems:
        if "is_subitem" in df.columns:
            df = df.drop(columns=["is_subitem", "subitem_text"])

    if columns:
        available_cols = set(df.columns)
        requested_cols = set(columns)
        missing_cols = requested_cols - available_cols
        if missing_cols:
            raise exceptions.monday_pandas_invalid_column_order(
                f"Columns not found: {missing_cols}"
            )
        df = df[columns]

    if filter_criteria:
        for col, value in filter_criteria.items():
            if col not in df.columns:
                raise exceptions.monday_pandas_invalid_column_order(
                    f"Filter column not found: {col}"
                )
            df = df[df[col] == value]

    return df
