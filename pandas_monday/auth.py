"""
Authentication utilities for pandas-monday.
"""

import os
from typing import Optional, Tuple

from . import exceptions


def get_credentials(
    api_token: Optional[str] = None,
    api_token_env_var: str = "MONDAY_API_TOKEN",
    verify_token: bool = True,
) -> Tuple[str, Optional[str]]:
    """
    Get Monday.com API credentials.

    This function attempts to get the Monday.com API token in the following order:
    1. From the provided api_token parameter
    2. From the environment variable specified by api_token_env_var

    Args:
        api_token: Optional API token string
        api_token_env_var: Name of environment variable containing the API token
        verify_token: Whether to verify the token with Monday.com API

    Returns:
        Tuple of (api_token, None). The second value is kept as None for
        compatibility with pandas-gbq pattern but could be used for workspace_id
        in the future.

    Raises:
        AuthenticationError: If no valid API token could be found or if the token
            verification fails.
    """
    token = api_token or os.environ.get(api_token_env_var)

    if not token:
        raise exceptions.monday_pandas_auth_error(
            f"No API token provided. Either pass api_token parameter or "
            f"set {api_token_env_var} environment variable."
        )

    if verify_token:
        _verify_api_token(token)

    return token, None


def _verify_api_token(token: str) -> None:
    """
    Verify that the API token is valid by making a test request.

    Args:
        token: Monday.com API token to verify

    Raises:
        AuthenticationError: If the token is invalid
    """
    import requests

    query = """
    query { me { name } }
    """

    try:
        response = requests.post(
            "https://api.monday.com/v2",
            json={"query": query},
            headers={
                "Authorization": token,
                "Content-Type": "application/json",
            },
            timeout=10,
        )

        if response.status_code == 401:
            raise exceptions.monday_pandas_auth_error("Invalid API token")
        elif response.status_code != 200:
            raise exceptions.monday_pandas_auth_error(
                f"API token verification failed with status {response.status_code}"
            )

        result = response.json()
        if "errors" in result:
            raise exceptions.monday_pandas_auth_error(
                f"API token verification failed: {result['errors']}"
            )

    except requests.exceptions.RequestException as e:
        raise exceptions.monday_pandas_auth_error(
            f"API token verification failed: {str(e)}"
        )
