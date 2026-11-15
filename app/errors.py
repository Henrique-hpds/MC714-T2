"""Erros de domínio expostos pela API."""

class ProtocolError(Exception):
    def __init__(self, status_code: int, detail: str, request_id: str | None = None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.request_id = request_id
