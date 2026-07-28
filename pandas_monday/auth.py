"""
Auth utilities.
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
    Get Monday.com credentials.
    """
    token = api_token or os.environ.get(api_token_env_var)

    if not token:
        raise exceptions.monday_pandas_auth_error(
            f"No API token. Provide param or set {api_token_env_var}."
        )

    if verify_token:
        _verify_api_token(token)

    return token, None


def _verify_api_token(token: str) -> None:
    """
    Verify token by test request.
    """
    import requests

    query = "query { me { name } }"

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
                f"Token verification failed with status {response.status_code}"
            )

        result = response.json()
        if "errors" in result:
            raise exceptions.monday_pandas_auth_error(
                f"Token verification failed: {result['errors']}"
            )

    except requests.exceptions.RequestException as e:
        raise exceptions.monday_pandas_auth_error(
            f"API token verification failed: {str(e)}"
        )
