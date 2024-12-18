from typing import Dict, Optional, Any
import requests
import pandas as pd
from . import exceptions


class monday_api:
    """Handles low-level API interactions with Monday.com"""

    BASE_URL = "https://api.monday.com/v2"
    API_VERSION = "2024-01"

    def __init__(self, api_token: str, user_agent: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": api_token,
                "Content-Type": "application/json",
                "API-Version": self.API_VERSION,
                "User-Agent": self._create_user_agent(user_agent),
            }
        )

    def _create_user_agent(self, user_agent: Optional[str] = None) -> str:
        """Creates a user agent string."""
        identity = f"pandas-monday-{pd.__version__}"
        return f"{user_agent} {identity}" if user_agent else identity

    def execute_query(
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
                raise exceptions.monday_pandas_api_error(
                    f"API request failed with status\n"
                    f"{response.status_code}: "
                    f"{response.text}"
                )

            result = response.json()
            if "errors" in result:
                raise exceptions.monday_pandas_api_error(
                    f"GraphQL query failed:\n" f"{result['errors']}"
                )

            return result
        except requests.exceptions.RequestException as e:
            raise exceptions.monday_pandas_api_error(f"Request failed: {str(e)}") from e

    _execute_query = execute_query
