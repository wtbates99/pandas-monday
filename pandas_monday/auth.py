"""
Authentication utilities for pandas-monday.
"""

import logging
import os
from typing import Optional, Tuple

from .exceptions import AuthenticationError

logger = logging.getLogger(__name__)

CREDENTIALS_CACHE_DIRNAME = "pandas_monday"
CREDENTIALS_CACHE_FILENAME = "monday_credentials.dat"


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
    3. From the cached credentials file

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
        token = _get_cached_credentials()

    if not token:
        raise AuthenticationError(
            f"No API token provided. Either pass api_token parameter or "
            f"set {api_token_env_var} environment variable."
        )

    if verify_token:
        _verify_api_token(token)

    # Cache the valid token
    _cache_credentials(token)

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
            raise AuthenticationError("Invalid API token")
        elif response.status_code != 200:
            raise AuthenticationError(
                f"API token verification failed with status {response.status_code}"
            )

        result = response.json()
        if "errors" in result:
            raise AuthenticationError(
                f"API token verification failed: {result['errors']}"
            )

    except requests.exceptions.RequestException as e:
        raise AuthenticationError(f"API token verification failed: {str(e)}")


def _get_cached_credentials() -> Optional[str]:
    """
    Retrieve cached API token from the credentials file.

    Returns:
        The cached API token or None if not found
    """
    import json
    from pathlib import Path

    cache_dir = Path.home() / ".cache" / CREDENTIALS_CACHE_DIRNAME
    cache_file = cache_dir / CREDENTIALS_CACHE_FILENAME

    if not cache_file.exists():
        return None

    try:
        with open(cache_file, "r") as f:
            data = json.load(f)
            return data.get("api_token")
    except (json.JSONDecodeError, IOError):
        logger.warning("Failed to read cached credentials")
        return None


def _cache_credentials(token: str) -> None:
    """
    Cache the API token to a file.

    Args:
        token: Monday.com API token to cache
    """
    import json
    from pathlib import Path

    cache_dir = Path.home() / ".cache" / CREDENTIALS_CACHE_DIRNAME
    cache_file = cache_dir / CREDENTIALS_CACHE_FILENAME

    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump({"api_token": token}, f)
    except IOError:
        logger.warning("Failed to cache credentials")
