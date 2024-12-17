import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import requests

from . import auth
from .exceptions import (
    MondayAPIError,
    MondayValidationError,
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

    def _start_timer(self):
        """Start timing an operation."""
        self.start = time.time()

    def get_elapsed_seconds(self) -> float:
        """Get elapsed time since start_timer was called."""
        return round(time.time() - self.start, 2)

    def log_elapsed_seconds(
        self, prefix: str = "Elapsed", postfix: str = "s.", overlong: int = 6
    ):
        """Log elapsed time if it exceeds overlong seconds."""
        sec = self.get_elapsed_seconds()
        if sec > overlong:
            logger.info(f"{prefix} {sec} {postfix}")

    def _create_user_agent(self, user_agent: Optional[str] = None) -> str:
        """Creates a user agent string."""
        identity = f"pandas-monday-{pd.__version__}"
        return f"{user_agent} {identity}" if user_agent else identity

    def _execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """Execute a GraphQL query against the Monday.com API."""
        try:
            response = self.session.post(
                self.BASE_URL,
                json={"query": query, "variables": variables or {}},
            )

            if response.status_code != 200:
                raise MondayAPIError(
                    f"API request failed with status {response.status_code}: {response.text}"
                )

            result = response.json()
            if "errors" in result:
                raise MondayAPIError(f"GraphQL query failed: {result['errors']}")

            return result
        except requests.exceptions.RequestException as e:
            raise MondayAPIError(f"Request failed: {str(e)}")

    def read_board(
        self,
        board_id: Union[str, int],
        columns: Optional[List[str]] = None,
        filter_criteria: Optional[Dict] = None,
        max_results: Optional[int] = None,
        progress_bar: bool = True,
    ) -> pd.DataFrame:

        query = """
        query ($board_id: ID!) {
            boards(ids: [$board_id]) {
                items {
                    id
                    name
                    column_values {
                        id
                        title
                        value
                        text
                    }
                }
            }
        }
        """

        variables = {"board_id": str(board_id)}
        response = self._execute_query(query, variables)

        if not response.get("data", {}).get("boards"):
            raise BoardNotFoundError(f"Board {board_id} not found")

        items = response["data"]["boards"][0]["items"]
        if max_results:
            items = items[:max_results]

        records = []
        iterator = (
            tqdm.tqdm(items) if progress_bar and tqdm and len(items) > 0 else items
        )

        for item in iterator:
            record = {"id": item["id"], "name": item["name"]}
            for col in item["column_values"]:
                record[col["title"]] = col["text"]
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

    def write_board(
        self,
        df: pd.DataFrame,
        board_id: Optional[Union[str, int]] = None,
        board_name: Optional[str] = None,
        workspace_id: Optional[str] = None,
        update_method: str = "append",
        chunksize: Optional[int] = None,
        progress_bar: bool = True,
    ) -> str:
        """
        Write a DataFrame to a Monday.com board.

        Args:
            df: DataFrame to write
            board_id: ID of existing board (for updates)
            board_name: Name for new board (if creating)
            workspace_id: Workspace ID (required for new boards)
            update_method: How to update existing boards ('append' or 'replace')
            chunksize: Number of rows to write at a time
            progress_bar: Whether to show a progress bar

        Returns:
            str: ID of the created or updated board
        """
        if board_id is None and (board_name is None or workspace_id is None):
            raise MondayValidationError(
                "Must provide either board_id for existing board "
                "or both board_name and workspace_id for new board"
            )

        self._start_timer()

        if board_id is None:
            board_id = self._create_board(board_name, workspace_id, df.columns)

        if update_method == "replace":
            self._clear_board(board_id)

        total_rows = len(df)
        if chunksize:
            chunks = np.array_split(df, np.ceil(total_rows / chunksize))
            if progress_bar and tqdm:
                chunks = tqdm.tqdm(chunks, desc="Writing to Monday.com")
            for chunk in chunks:
                self._add_items_to_board(board_id, chunk)
        else:
            self._add_items_to_board(board_id, df, progress_bar=progress_bar)

        return board_id

    def _create_board(self, name: str, workspace_id: str, columns: List[str]) -> str:
        """Create a new board with the specified columns."""
        query = """
        mutation ($name: String!, $workspace_id: ID!) {
            create_board(
                board_name: $name,
                workspace_id: $workspace_id,
            ) {
                id
            }
        }
        """

        variables = {"name": name, "workspace_id": workspace_id}

        response = self._execute_query(query, variables)
        return response["data"]["create_board"]["id"]

    def _clear_board(self, board_id: Union[str, int]):
        """Remove all items from a board."""
        query = """
        mutation ($board_id: ID!) {
            delete_board(board_id: $board_id) {
                id
            }
        }
        """

        self._execute_query(query, {"board_id": str(board_id)})

    def _add_items_to_board(
        self, board_id: Union[str, int], df: pd.DataFrame, progress_bar: bool = True
    ):
        """Add items from DataFrame to board."""
        iterator = (
            tqdm.tqdm(df.iterrows())
            if progress_bar and tqdm and len(df) > 0
            else df.iterrows()
        )
        for _, row in iterator:
            self._create_item(board_id, row)

    def _create_item(self, board_id: Union[str, int], row: pd.Series):
        """Create a single item in a board."""
        query = """
        mutation ($board_id: ID!, $item_name: String!, $column_values: JSON!) {
            create_item(
                board_id: $board_id,
                item_name: $item_name,
                column_values: $column_values
            ) {
                id
            }
        }
        """

        variables = {
            "board_id": str(board_id),
            "item_name": str(row.get("name", "New Item")),
            "column_values": self._prepare_column_values(row),
        }

        self._execute_query(query, variables)

    def _prepare_column_values(self, row: pd.Series) -> Dict:
        """Convert DataFrame row values to Monday.com column format."""
        # Skip special columns
        skip_columns = {"id", "name"}

        column_values = {}
        for col, value in row.items():
            if col in skip_columns:
                continue
            if pd.isna(value):
                continue
            column_values[col] = str(value)

        return column_values
