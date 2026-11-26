import logging

from requests.adapters import HTTPAdapter
from urllib3.response import BaseHTTPResponse
from urllib3.util import Retry

logger = logging.getLogger(__name__)

class LogRetry(Retry):
    def sleep(self, response: BaseHTTPResponse | None = None) -> None:
        if self.respect_retry_after_header and response and (retry_after := self.get_retry_after(response)):
            logger.warning(f"Waiting for {retry_after} seconds to retry the request")
        elif (backoff := self.get_backoff_time()) > 0:
            logger.warning(f"Waiting for {backoff} seconds to retry the request")
        return super().sleep(response)

HTTP_ADAPTER = HTTPAdapter(max_retries=LogRetry(
    status_forcelist=[429, 500, 502, 503],
    backoff_factor=10,
    backoff_max=Retry.DEFAULT_RETRY_AFTER_MAX,
))
