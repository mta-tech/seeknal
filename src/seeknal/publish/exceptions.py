from __future__ import annotations


class PublishAuthError(Exception):
    pass


class PublishTooLargeError(Exception):
    pass


class PublishContentTypeError(Exception):
    pass


class PublishServerError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"HTTP {status_code}: {detail}")


class PackageTooLargeError(ValueError):
    pass
