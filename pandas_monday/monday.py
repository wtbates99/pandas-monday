from typing import Dict, List, Optional, Union, Any

import pandas as pd
import requests

from . import auth
from .exceptions import (
    MondayAPIError,
    InvalidColumnOrder,
    BoardNotFoundError,
)

try:
    import tqdm  # noqa
except ImportError:
    tqdm = None


class MondayClient:
    """
    A client for interacting with Monday.com's API using pandas DataFrames.
    """

    BASE_URL = "https://api.monday.com/v2"

    def __init__(
        self,
        api_token: Optional[str] = None,
        user_agent: Optional[str] = None,
        verify_token: bool = True,
    ):
        """
        Initialize the Monday.com client.

        Args:
        api_token: Your Monday.com API token. If not provided, will attempt to
        get it from MONDAY_API_TOKEN environment variable or cached credentials.
        user_agent: Custom user agent string
        verify_token: Whether to verify the token with Monday.com API
        """
        self.api_token, _ = auth.get_credentials(
            api_token=api_token, verify_token=verify_token
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": self.api_token,
                "Content-Type": "application/json",
                "API-Version": "2024-01",
                "User-Agent": self._create_user_agent(user_agent),
            }
        )

    def _create_user_agent(self, user_agent: Optional[str] = None) -> str:
        """Creates a user agent string."""
        identity = f"pandas-monday-{pd.__version__}"
        return f"{user_agent} {identity}" if user_agent else identity

    def _execute_query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a GraphQL query against the Monday.com API."""
        try:
            response = self.session.post(
                self.BASE_URL,
                json={"query": query, "variables": variables or {}},
            )

            if response.status_code != 200:
                raise MondayAPIError(
                    f"API request failed with status\n"
                    f"{response.status_code}: "
                    f"{response.text}"
                )

            result = response.json()
            if "errors" in result:
                raise MondayAPIError(f"GraphQL query failed:\n" f"{result['errors']}")

            return result
        except requests.exceptions.RequestException as e:
            raise MondayAPIError(f"Request failed: {str(e)}") from e

    def read_board(
        self,
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
        meta_response = self._execute_query(meta_query, {"board_id": str(board_id)})

        if not meta_response.get("data", {}).get("boards"):
            raise BoardNotFoundError(f"Board {board_id} not found")

        board = meta_response["data"]["boards"][0]
        column_mapping = {col["id"]: col["title"] for col in board["columns"]}

        # Then, fetch items with pagination
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

                response = self._execute_query(items_query, variables)
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
                raise InvalidColumnOrder(f"Columns not found: {missing_cols}")
            df = df[columns]

        if filter_criteria:
            for col, value in filter_criteria.items():
                if col not in df.columns:
                    raise InvalidColumnOrder(f"Filter column not found: {col}")
                df = df[df[col] == value]

        return df


if __name__ == "__main__":
    from pandas_monday.monday import MondayClient

    BOARD_ID = "6225157636"

    df = MondayClient().read_board(
        board_id=BOARD_ID,
        # Optional parameters:
        # columns=['name', 'Status', 'Priority'],  # Specify columns you want
        # filter_criteria={'Status': 'Done'},      # Filter rows
        # max_results=100,                         # Limit number of rows
        include_subitems=False,
        progress_bar=True,
    )

    df.to_csv("monday_board_export.csv", index=False)
