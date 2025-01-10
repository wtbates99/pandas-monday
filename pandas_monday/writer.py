from typing import Dict, Optional, Union, Any
import pandas as pd
import tqdm
import time
from . import exceptions
import json


def write_board(
    executor,
    board_id: Union[str, int],
    df: pd.DataFrame,
    mode: str = "append",
    overwrite_type: str = "archive",
    group_column: Optional[str] = None,
    progress_bar: bool = True,
) -> None:
    """Write a DataFrame to a Monday.com board with improved error handling."""
    try:
        if mode not in ["append", "replace"]:
            raise ValueError("Mode must be either 'append' or 'replace'")
        if overwrite_type not in ["archive", "delete"]:
            raise ValueError("overwrite_type must be either 'archive' or 'delete'")

        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                board_metadata = _fetch_board_metadata(executor, board_id)
                break
            except exceptions.monday_pandas_api_error as e:
                retry_count += 1
                if retry_count == max_retries:
                    raise exceptions.monday_pandas_api_error(
                        f"Failed to fetch board metadata after {max_retries} attempts: {str(e)}"
                    )
                time.sleep(1)

        column_mapping = {col["title"]: col["id"] for col in board_metadata["columns"]}
        group_mapping = {
            group["title"]: group["id"] for group in board_metadata["groups"]
        }

        missing_columns = set(df.columns) - set(column_mapping.keys())
        if missing_columns:
            raise exceptions.monday_pandas_invalid_column_order(
                f"Columns not found in board: {missing_columns}"
            )

        # If mode is "replace", clear all existing items
        if mode == "replace":
            _clear_board_items(executor, overwrite_type, board_id, progress_bar)

        chunk_size = 10
        for i in range(0, len(df), chunk_size):
            df_chunk = df.iloc[i : i + chunk_size]
            _add_items_to_board(
                executor,
                board_id,
                df_chunk,
                column_mapping,
                group_mapping,
                group_column,
                progress_bar,
            )

    except Exception as e:
        raise exceptions.monday_pandas_api_error(f"Error writing to board: {str(e)}")


# 2. Improved Item Creation
def _add_items_to_board(
    executor,
    board_id: Union[str, int],
    df: pd.DataFrame,
    column_mapping: Dict[str, str],
    group_mapping: Dict[str, str],
    group_column: Optional[str] = None,
    progress_bar: bool = True,
) -> None:
    """Add items from a DataFrame to a board with improved error handling."""
    create_mutation = """
    mutation ($boardId: ID!, $groupId: String, $itemName: String!, $columnValues: JSON!) {
        create_item (
            board_id: $boardId,
            group_id: $groupId,
            item_name: $itemName,
            column_values: $columnValues
        ) {
            id
        }
    }
    """

    retry_delay = 1
    max_retries = 3

    try:
        with tqdm.tqdm(disable=not progress_bar, total=len(df), unit=" items") as pbar:
            for _, row in df.iterrows():
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        column_values = {}
                        for col in df.columns:
                            if (
                                col == "name"
                            ):  # Skip name column as it's handled separately
                                continue

                            value = row[col]
                            if pd.isna(value) or value == "":
                                continue

                            col_id = column_mapping[col]

                            # Modified column type handling
                            if "status" in col.lower():
                                # Status values need to be sent as a label object
                                column_values[col_id] = {"label": str(value)}
                            elif col == "Due date" and value:
                                try:
                                    if isinstance(value, str):
                                        parsed_date = pd.to_datetime(value)
                                    else:
                                        parsed_date = pd.Timestamp(value)
                                    column_values[col_id] = {
                                        "date": parsed_date.strftime("%Y-%m-%d")
                                    }
                                except Exception:
                                    continue
                            elif col == "Assignee" and value:
                                try:
                                    user_id = str(int(value))
                                    column_values[col_id] = {
                                        "personsAndTeams": [{"id": user_id}]
                                    }
                                except ValueError:
                                    continue
                            else:
                                # For text and other simple fields
                                column_values[col_id] = str(value)

                        # Handle group assignment
                        group_id = None
                        if group_column and group_column in df.columns:
                            group_name = row[group_column]
                            if group_name in group_mapping:
                                group_id = group_mapping[group_name]

                        variables = {
                            "boardId": str(
                                board_id
                            ),  # Changed from board_id to boardId
                            "groupId": group_id,  # Changed from group_id to groupId
                            "itemName": row.get(
                                "name", "New Item"
                            ),  # Changed from item_name to itemName
                            "columnValues": json.dumps(
                                column_values
                            ),  # Changed from column_values to columnValues
                        }

                        response = executor._execute_query(create_mutation, variables)

                        if not response.get("data", {}).get("create_item"):
                            raise exceptions.monday_pandas_api_error(
                                f"Failed to create item. Response: {response}"
                            )

                        break  # Success, exit retry loop

                    except exceptions.monday_pandas_api_error as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            raise exceptions.monday_pandas_api_error(
                                f"Failed to create item after {max_retries} attempts. "
                                f"Last error: {str(e)}. "
                                f"Variables: {variables}"
                            )
                        time.sleep(retry_delay)

                pbar.update(1)
                pbar.set_description(f"Adding items to board {board_id}")

    except Exception as e:
        raise exceptions.monday_pandas_api_error(
            f"Error adding items to board: {str(e)}"
        )


def _fetch_board_metadata(executor, board_id: Union[str, int]) -> Dict[str, Any]:
    """Fetch metadata (columns, groups, etc.) for a board."""
    meta_query = """
    query ($board_id: ID!) {
        boards(ids: [$board_id]) {
            name
            columns { id title }
            groups { id title }
        }
    }
    """
    meta_response = executor._execute_query(meta_query, {"board_id": str(board_id)})

    if not meta_response.get("data", {}).get("boards"):
        raise exceptions.monday_pandas_board_not_found_error(
            f"Board {board_id} not found"
        )

    return meta_response["data"]["boards"][0]


def _clear_board_items(
    executor, mutation_state: str, board_id: Union[str, int], progress_bar: bool = True
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
    if mutation_state == "archive":
        mutation = """
        mutation ($item_id: ID!) {
            archive_item (item_id: $item_id) { id }
        }
        """
    elif mutation_state == "delete":
        mutation = """
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
                    executor._execute_query(mutation, {"item_id": item["id"]})
                    total_items += 1
                    pbar.update(1)
                    pbar.set_description(f"Mutating items from board {board_id}")

                cursor = items_page.get("cursor")
                if not cursor or not items:
                    break

    except Exception as e:
        raise exceptions.monday_pandas_api_error(
            f"Error clearing board items: {str(e)}"
        )
