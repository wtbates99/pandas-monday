"""Custom exceptions."""


class monday_pandas_auth_error(Exception):
    """Auth error."""


class monday_pandas_api_error(Exception):
    """API error."""


class monday_pandas_invalid_column_order(Exception):
    """Invalid col order."""


class monday_pandas_board_not_found_error(Exception):
    """Board not found."""
