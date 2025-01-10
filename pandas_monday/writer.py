from typing import Dict, Optional, Union
import pandas as pd
import tqdm
from . import exceptions


def write_board(
    executor,
    board_id: Union[str, int],
    df: pd.DataFrame,
    mode: str = "append",
    group_id: Optional[str] = None,
    progress_bar: bool = True,
) -> None:
    """Write a DataFrame to a Monday.com board.

    Args:
        executor: monday_pandas client instance
        board_id: Unique identifier of the board to write to
        df: DataFrame containing the data to write
        mode: Write mode, either "append" or "replace"
        group_id: Optional group ID to add items to a specific group
        progress_bar: Whether to display a progress bar during writing

    Raises:
        monday_pandas_board_not_found_error: If the specified board doesn't exist
        monday_pandas_invalid_column_order: If DataFrame columns don't match board columns
        monday_pandas_api_error: If there's an error during API calls
    """
    # Validate mode
    if mode not in ["append", "replace"]:
        raise ValueError("Mode must be either 'append' or 'replace'")

    # Fetch board metadata to validate columns
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
    column_mapping = {col["title"]: col["id"] for col in board["columns"]}

    # Validate DataFrame columns
    missing_columns = set(df.columns) - set(column_mapping.keys())
    if missing_columns:
        raise exceptions.monday_pandas_invalid_column_order(
            f"Columns not found in board: {missing_columns}"
        )

    # If mode is "replace", clear all existing items
    if mode == "replace":
        _clear_board_items(executor, board_id, progress_bar)

    # Add items to the board
    _add_items_to_board(executor, board_id, df, column_mapping, group_id, progress_bar)


def _clear_board_items(
    executor, board_id: Union[str, int], progress_bar: bool = True
) -> None:
    """Clear all items from a board."""
    items_query = """
    query ($board_id: ID!, $cursor: String, $page_size: Int!) {
        boards(ids: [$board_id]) {
            items_page(limit: $page_size, cursor: $cursor) {
                cursor
                items { id }
            }
        }
    }
    """
    delete_mutation = """
    mutation ($item_id: ID!) {
        delete_item (item_id: $item_id) { id }
    }
    """

    cursor = None
    total_items = 0

    try:
        with tqdm.tqdm(disable=not progress_bar, unit=" items") as pbar:
            while True:
                variables = {
                    "board_id": str(board_id),
                    "cursor": cursor,
                    "page_size": 100,
                }

                response = executor._execute_query(items_query, variables)
                items_page = response["data"]["boards"][0]["items_page"]
                items = items_page["items"]

                for item in items:
                    executor._execute_query(delete_mutation, {"item_id": item["id"]})
                    total_items += 1
                    pbar.update(1)
                    pbar.set_description(f"Deleting items from board {board_id}")

                cursor = items_page.get("cursor")
                if not cursor or not items:
                    break

    except Exception as e:
        raise exceptions.monday_pandas_api_error(
            f"Error clearing board items: {str(e)}"
        )


def _add_items_to_board(
    executor,
    board_id: Union[str, int],
    df: pd.DataFrame,
    column_mapping: Dict[str, str],
    group_id: Optional[str] = None,
    progress_bar: bool = True,
) -> None:
    """Add items from a DataFrame to a board."""
    create_mutation = """
    mutation ($board_id: ID!, $group_id: String, $item_name: String!, $column_values: JSON!) {
        create_item (board_id: $board_id, group_id: $group_id, item_name: $item_name, column_values: $column_values) { id }
    }
    """

    try:
        with tqdm.tqdm(disable=not progress_bar, total=len(df), unit=" items") as pbar:
            for _, row in df.iterrows():
                column_values = {
                    column_mapping[col]: str(row[col]) for col in df.columns
                }

                variables = {
                    "board_id": str(board_id),
                    "group_id": group_id,
                    "item_name": row.get("name", "New Item"),
                    "column_values": column_values,
                }

                executor._execute_query(create_mutation, variables)
                pbar.update(1)
                pbar.set_description(f"Adding items to board {board_id}")

    except Exception as e:
        raise exceptions.monday_pandas_api_error(
            f"Error adding items to board: {str(e)}"
        )
