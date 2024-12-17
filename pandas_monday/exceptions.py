class AuthenticationError(Exception):
    """Raised when there is an error in the Monday.com API response."""

    pass


class MondayError(Exception):
    """Base exception for all pandas-monday errors."""

    pass


class MondayAPIError(MondayError):
    """Raised when there is an error in the Monday.com API response."""

    pass


class InvalidColumnOrder(MondayError):
    """Raised when the provided column order does not match the available columns."""

    pass


class BoardNotFoundError(MondayError):
    """Raised when the specified Monday.com board cannot be found."""

    pass
