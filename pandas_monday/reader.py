from typing import Any, Dict, List, Optional, Union

import pandas as pd

from . import api, exceptions


def read_board(
    executor: api.monday_api,
    board_id: Union[str, int],
    columns: Optional[List[str]] = None,
    max_results: Optional[int] = None,
    include_subitems: bool = False,
    page_size: int = 100,
) -> pd.DataFrame:
    """Read board -> DataFrame."""
    meta_q = """query($board_id: ID!) { boards(ids: [$board_id]) { name columns { id title } } }"""
    meta_r = executor._execute_query(meta_q, {"board_id": str(board_id)})
    boards = meta_r.get("data", {}).get("boards", [])
    if not boards:
        raise exceptions.monday_pandas_board_not_found_error(
            f"Board {board_id} not found"
        )
    board = boards[0]
    column_mapping = {c["id"]: c["title"] for c in board["columns"]}
    items_q = """query($board_id: ID!, $cursor: String, $page_size: Int!) {
      boards(ids: [$board_id]) {
        items_page(limit: $page_size, cursor: $cursor) {
          cursor
          items {
            id name group { title }
            column_values { id text }
            subitems {
              id name
              column_values { id text }
            }
          }
        }
      }
    }"""

    def _process_item(
        item: Dict[str, Any],
        is_subitem: bool = False,
        parent: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        r = {
            "board_id": item["id"],
            "board_name": board["name"],
            "group": (
                parent["group"]["title"]
                if is_subitem and parent and parent.get("group")
                else item.get("group", {}).get("title")
            ),
            "board_item": parent.get("name") if is_subitem and parent else item["name"],
            "is_subitem": is_subitem,
            "subitem_text": item["name"] if is_subitem else None,
        }
        for col in item["column_values"]:
            col_title = column_mapping.get(col["id"], col["id"])
            r[col_title] = col["text"]
        return r

    recs, cur, total = [], None, 0
    try:
        while True:
            vars = {
                "board_id": str(board_id),
                "cursor": cur,
                "page_size": (
                    min(page_size, max_results - total) if max_results else page_size
                ),
            }
            resp = executor._execute_query(items_q, vars)
            page = resp["data"]["boards"][0]["items_page"]
            items = page["items"]
            for it in items:
                recs.append(_process_item(it))
                if include_subitems and it.get("subitems"):
                    recs.extend([_process_item(s, True, it) for s in it["subitems"]])
            total += len(items)
            if max_results and total >= max_results:
                recs = recs[:max_results]
                break
            cur = page.get("cursor")
            if not cur or not items:
                break
    except Exception as e:
        raise exceptions.monday_pandas_api_error(f"Error fetching board data: {str(e)}")
    df = pd.DataFrame.from_records(recs)
    cols_to_drop = ["Subitems"]
    if not include_subitems:
        cols_to_drop += ["is_subitem", "subitem_text"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    if columns:
        _validate_columns(df, columns)
        df = df[columns]
    return df


def _validate_columns(df: pd.DataFrame, columns: Union[List[str], List[Any]]) -> None:
    """Validate columns exist."""
    av, req = set(df.columns), set(columns)
    miss = req - av
    if miss:
        raise exceptions.monday_pandas_invalid_column_order(
            f"Columns not found: {miss}"
        )
