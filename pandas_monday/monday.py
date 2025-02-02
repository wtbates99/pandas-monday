from typing import Any, Dict, Optional

import pandas as pd

from . import (
    api,
    auth,
    reader,
    writer,
)


class monday_pandas:
    """
    A client for interacting with Monday.com's API using pandas DataFrames.
    """

    def __init__(
        self,
        api_token: Optional[str] = None,
        user_agent: Optional[str] = None,
        verify_token: bool = True,
    ) -> None:
        """
        Initialize the monday_pandas client.

        Args:
        api_token: Your Monday.com API token. If not provided, will attempt to
        get it from MONDAY_API_TOKEN environment variable or cached credentials.
        user_agent: Custom user agent string
        verify_token: Whether to verify the token with Monday.com API
        """
        token, _ = auth.get_credentials(api_token=api_token, verify_token=verify_token)
        self.api = api.monday_api(token, user_agent)

    def _execute_query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a GraphQL query against the Monday.com API."""
        return self.api.execute_query(query, variables)

    def read_board(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Read a board from Monday.com and return it as a DataFrame."""
        return reader.read_board(self.api, *args, **kwargs)

    def write_board(self, *args: Any, **kwargs: Any) -> None:
        """Write a DataFrame to a Monday.com board."""
        return writer.write_board(self.api, *args, **kwargs)
