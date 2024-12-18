from typing import Dict, List, Optional, Union, Any
import pandas as pd
import tqdm
from . import exceptions
from . import api


def read_board(
    executor: api.monday_api,
    board_id: Union[str, int],
    columns: Optional[List[str]] = None,
    max_results: Optional[int] = None,
    progress_bar: bool = True,
    include_subitems: bool = False,
    page_size: int = 100,
) -> pd.DataFrame:
    """Read a board from Monday.com and return it as a DataFrame.

    Args:
        executor: monday_api executor instance
        board_id: Unique identifier of the board to read
        columns: Optional list of columns to include in the output
        filter_criteria: Optional dictionary of column-value pairs to filter results
        max_results: Optional maximum number of results to return
        progress_bar: Whether to display a progress bar during fetching
        include_subitems: Whether to include subitems in the output
        page_size: Number of items to fetch per API request

    Returns:
        pd.DataFrame: DataFrame containing the board data

    Raises:
        monday_pandas_board_not_found_error: If the specified board doesn't exist
        monday_pandas_invalid_column_order: If requested columns don't exist
    """
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

    def _process_item(
        item: Dict[str, Any],
        is_subitem: bool = False,
        parent_item: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Process a single item or subitem and return a formatted record."""
        record = {
            "board_id": item["id"],
            "board_name": board["name"],
            "group": (
                parent_item["group"]["title"] if is_subitem else item["group"]["title"]
            ),
            "board_item": parent_item["name"] if is_subitem else item["name"],
            "is_subitem": is_subitem,
            "subitem_text": item["name"] if is_subitem else None,
        }

        for col in item["column_values"]:
            col_title = column_mapping.get(col["id"], col["id"])
            record[col_title] = col["text"]

        return record

    records = []
    cursor = None
    total_items = 0

    try:
        with tqdm.tqdm(disable=not (progress_bar and tqdm), unit=" items") as pbar:
            while True:
                variables = {
                    "board_id": str(board_id),
                    "cursor": cursor,
                    "page_size": (
                        min(page_size, max_results - total_items)
                        if max_results
                        else page_size
                    ),
                }

                response = executor._execute_query(items_query, variables)
                items_page = response["data"]["boards"][0]["items_page"]
                items = items_page["items"]

                for item in items:
                    records.append(_process_item(item))

                    if include_subitems and item.get("subitems"):
                        records.extend(
                            [
                                _process_item(
                                    subitem, is_subitem=True, parent_item=item
                                )
                                for subitem in item["subitems"]
                            ]
                        )

                total_items += len(items)
                pbar.update(len(items))
                pbar.set_description(f"Fetching items from board {board_id}")

                if max_results and total_items >= max_results:
                    records = records[:max_results]
                    break

                cursor = items_page.get("cursor")
                if not cursor or not items:
                    break

    except Exception as e:
        raise exceptions.monday_pandas_api_error(f"Error fetching board data: {str(e)}")

    df = pd.DataFrame.from_records(records)

    columns_to_drop = ["Subitems"]
    if not include_subitems:
        columns_to_drop.extend(["is_subitem", "subitem_text"])
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    if columns:
        _validate_columns(df, columns)
        df = df[columns]

    return df


def _validate_columns(
    df: pd.DataFrame, columns: Union[List[str], Dict[str, Any].keys]
) -> None:
    """Validate that all requested columns exist in the DataFrame."""
    available_cols = set(df.columns)
    requested_cols = set(columns)
    missing_cols = requested_cols - available_cols
    if missing_cols:
        raise exceptions.monday_pandas_invalid_column_order(
            f"Columns not found: {missing_cols}"
        )
