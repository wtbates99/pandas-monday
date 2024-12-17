from typing import Dict, Optional, Any

from . import api
from . import auth
from . import reader


class monday_pandas:
    """
    A client for interacting with Monday.com's API using pandas DataFrames.
    """

    def __init__(
        self,
        api_token: Optional[str] = None,
        user_agent: Optional[str] = None,
        verify_token: bool = True,
    ):
        """
        Initialize the monday_pandas client.

        Args:
        api_token: Your Monday.com API token. If not provided, will attempt to
        get it from MONDAY_API_TOKEN environment variable or cached credentials.
        user_agent: Custom user agent string
        verify_token: Whether to verify the token with Monday.com API
        """
        api_token, _ = auth.get_credentials(
            api_token=api_token, verify_token=verify_token
        )
        self.api = api.monday_api(api_token, user_agent)

    def _execute_query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a GraphQL query against the Monday.com API."""
        return self.api.execute_query(query, variables)

    def read_board(self, *args, **kwargs):
        """Read a board from Monday.com and return it as a DataFrame."""
        return reader.read_board(self, *args, **kwargs)
