from typing import Dict, Optional, Any

import pandas as pd
import requests
from .reader import read_board

from . import auth
from .exceptions import (
    MondayAPIError,
)


class monday_pandas:
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

    def read_board(self, *args, **kwargs):
        """Read a board from Monday.com and return it as a DataFrame."""
        return read_board(self, *args, **kwargs)
