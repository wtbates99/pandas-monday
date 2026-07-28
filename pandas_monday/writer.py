import json
import time
from typing import Any, Dict, Optional, Union

import pandas as pd

from . import exceptions
from .api import monday_api


def write_board(
    executor: monday_api,
    board_id: Union[str, int],
    df: pd.DataFrame,
    mode: str = "append",
    overwrite_type: str = "archive",
    group_column: Optional[str] = None,
) -> None:
    """Write DataFrame to Monday board."""
    try:
        if mode not in ["append", "replace"]:
            raise ValueError("mode must be 'append' or 'replace'")
        if overwrite_type not in ["archive", "delete"]:
            raise ValueError("overwrite_type must be 'archive' or 'delete'")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                board_metadata = _fetch_board_metadata(executor, board_id)
                break
            except exceptions.monday_pandas_api_error as e:
                if attempt == max_retries - 1:
                    raise exceptions.monday_pandas_api_error(
                        f"Failed board metadata after {max_retries} attempts: {str(e)}"
                    )
                time.sleep(1)
        col_map = {c["title"]: c["id"] for c in board_metadata["columns"]}
        grp_map = {g["title"]: g["id"] for g in board_metadata["groups"]}
        miss = set(df.columns) - set(col_map.keys())
        if miss:
            raise exceptions.monday_pandas_invalid_column_order(
                f"Missing board columns: {miss}"
            )
        if mode == "replace":
            _clear_board_items(executor, overwrite_type, board_id)
        chunk_size = 10
        for i in range(0, len(df), chunk_size):
            _add_items_to_board(
                executor,
                board_id,
                df.iloc[i : i + chunk_size],
                col_map,
                grp_map,
                group_column,
            )
    except Exception as e:
        raise exceptions.monday_pandas_api_error(f"Error writing to board: {str(e)}")


def _add_items_to_board(
    executor: monday_api,
    board_id: Union[str, int],
    df: pd.DataFrame,
    col_map: Dict[str, str],
    grp_map: Dict[str, str],
    group_column: Optional[str] = None,
) -> None:
    """Add items from DF."""
    create_mut = """
    mutation($boardId: ID!, $groupId: String, $itemName: String!, $columnValues: JSON!) {
      create_item(board_id: $boardId, group_id: $groupId, item_name: $itemName, column_values: $columnValues) { id }
    }
    """
    retry_delay, max_r = 1, 3
    for _, row in df.iterrows():
        rcount = 0
        while rcount < max_r:
            try:
                col_vals: Dict[str, Union[str, Dict[str, Any]]] = {}
                for col in df.columns:
                    if col == "name":
                        continue
                    val = row[col]
                    if pd.isna(val) or val == "":
                        continue
                    cid = col_map[col]
                    if "status" in col.lower():
                        col_vals[cid] = {"label": str(val)}
                    elif col == "Due date" and val:
                        try:
                            pdate = (
                                pd.to_datetime(val)
                                if isinstance(val, str)
                                else pd.Timestamp(val)
                            )
                            col_vals[cid] = {"date": pdate.strftime("%Y-%m-%d")}
                        except:
                            continue
                    elif col == "Assignee" and val:
                        try:
                            user_id = str(int(val))
                            col_vals[cid] = {"personsAndTeams": [{"id": user_id}]}
                        except ValueError:
                            continue
                    else:
                        col_vals[cid] = str(val)
                grp_id = None
                if group_column and group_column in df.columns:
                    gname = row[group_column]
                    if gname in grp_map:
                        grp_id = grp_map[gname]
                variables = {
                    "boardId": str(board_id),
                    "groupId": grp_id,
                    "itemName": row.get("name", "New Item"),
                    "columnValues": json.dumps(col_vals),
                }
                resp = executor._execute_query(create_mut, variables)
                if not resp.get("data", {}).get("create_item"):
                    raise exceptions.monday_pandas_api_error(
                        f"Failed to create item: {resp}"
                    )
                break
            except exceptions.monday_pandas_api_error as e:
                rcount += 1
                if rcount == max_r:
                    raise exceptions.monday_pandas_api_error(
                        f"Create item fail after {max_r} attempts. {str(e)}. Vars: {variables}"
                    )
                time.sleep(retry_delay)


def _fetch_board_metadata(
    executor: monday_api, board_id: Union[str, int]
) -> Dict[str, Any]:
    """Fetch board metadata."""
    meta_q = """query($board_id: ID!) { boards(ids: [$board_id]) { name columns { id title } groups { id title } } }"""
    r = executor._execute_query(meta_q, {"board_id": str(board_id)})
    if not r.get("data", {}).get("boards"):
        raise exceptions.monday_pandas_board_not_found_error(
            f"Board {board_id} not found"
        )
    return r["data"]["boards"][0]


def _clear_board_items(
    executor: monday_api, mutation_state: str, board_id: Union[str, int]
) -> None:
    """Clear all items from board."""
    items_q = """query($board_id: ID!, $cursor: String, $page_size: Int!) {
      boards(ids: [$board_id]) {
        items_page(limit: $page_size, cursor: $cursor) {
          cursor
          items { id }
        }
      }
    }"""
    mut_archive = (
        """mutation($item_id: ID!) { archive_item(item_id: $item_id) { id } }"""
    )
    mut_delete = """mutation($item_id: ID!) { delete_item(item_id: $item_id) { id } }"""
    mutation = mut_archive if mutation_state == "archive" else mut_delete
    c = None
    try:
        while True:
            vars = {"board_id": str(board_id), "cursor": c, "page_size": 100}
            resp = executor._execute_query(items_q, vars)
            page = resp["data"]["boards"][0]["items_page"]
            items = page["items"]
            for it in items:
                executor._execute_query(mutation, {"item_id": it["id"]})
            c = page.get("cursor")
            if not c or not items:
                break
    except Exception as e:
        raise exceptions.monday_pandas_api_error(f"Error clearing board: {str(e)}")
