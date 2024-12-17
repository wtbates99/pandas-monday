class monday_pandas_auth_error(Exception):
    """Raised when there is an error in the Monday.com API response."""

    pass


class monday_pandas_api_error(Exception):
    """Raised when there is an error in the Monday.com API response."""

    pass


class monday_pandas_invalid_column_order(Exception):
    """Raised when the provided column order does not match the available columns."""

    pass


class monday_pandas_board_not_found_error(Exception):
    """Raised when the specified Monday.com board cannot be found."""

    pass
