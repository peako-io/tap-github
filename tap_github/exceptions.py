# Python imports
# Third-Party imports
# Project imports

class GithubException(Exception):
    pass


class BadCredentialsException(GithubException):
    pass


class AuthException(GithubException):
    pass


class NotFoundException(GithubException):
    pass


class BadRequestException(GithubException):
    pass


class InternalServerError(GithubException):
    pass


class UnprocessableError(GithubException):
    pass


class NotModifiedError(GithubException):
    pass


class MovedPermanentlyError(GithubException):
    pass


class ConflictError(GithubException):
    pass


class RateLimitExceeded(GithubException):
    pass


class DependencyException(Exception):
    pass


class SchemaFileFormatException(Exception):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    301: {
        "raise_exception": MovedPermanentlyError,
        "message": "The resource you are looking for is moved to another URL."
    },
    304: {
        "raise_exception": NotModifiedError,
        "message": "The requested resource has not been modified since the last time you accessed it."
    },
    400: {
        "raise_exception": BadRequestException,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": BadCredentialsException,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": AuthException,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": NotFoundException,
        "message": "The resource you have specified cannot be found"
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server."
    },
    422: {
        "raise_exception": UnprocessableError,
        "message": "The request was not able to process right now."
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "An error has occurred at Github's end."
    }
}
