import logging
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

logger = logging.getLogger(__name__)


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
    ) -> pd.DataFrame:
        """Read a board from Monday.com and return it as a DataFrame."""
        query = """
        query ($board_id: ID!) {
            boards(ids: [$board_id]) {
                items_page {
                    items {
                        id
                        name
                        column_values {
                            id
                            text
                            value
                        }
                    }
                }
            }
        }
        """

        variables = {"board_id": str(board_id)}
        response = self._execute_query(query, variables)
        print(response)

        if not response.get("data", {}).get("boards"):
            raise BoardNotFoundError(f"Board {board_id} not found")

        items = response["data"]["boards"][0]["items_page"]["items"]
        if max_results:
            items = items[:max_results]

        records = []
        iterator = (
            tqdm.tqdm(items) if progress_bar and tqdm and len(items) > 0 else items
        )

        for item in iterator:
            record = {"id": item["id"], "name": item["name"]}
            for col in item["column_values"]:
                if col["text"]:
                    record[col["id"]] = col["text"]
                elif col["value"]:
                    record[col["id"]] = col["value"]
                else:
                    record[col["id"]] = None
            records.append(record)

        df = pd.DataFrame.from_records(records)

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
