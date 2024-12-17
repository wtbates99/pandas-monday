class MondayError(Exception):
    """Base exception for all pandas-monday errors."""

    pass


class MondayAPIError(MondayError):
    """Raised when there is an error in the Monday.com API response."""

    pass


class MondayValidationError(MondayError):
    """Raised when there is a validation error in the input parameters."""

    pass


class InvalidColumnOrder(MondayError):
    """Raised when the provided column order does not match the available columns."""

    pass


class InvalidSchema(MondayError):
    """Raised when there is a mismatch between DataFrame and Monday.com column types."""

    pass


class BoardNotFoundError(MondayError):
    """Raised when the specified Monday.com board cannot be found."""

    pass


class WorkspaceNotFoundError(MondayError):
    """Raised when the specified Monday.com workspace cannot be found."""

    pass


class RateLimitExceededError(MondayAPIError):
    """Raised when the Monday.com API rate limit is exceeded."""

    pass


class AuthenticationError(MondayAPIError):
    """Raised when there is an authentication error with the Monday.com API."""

    pass
