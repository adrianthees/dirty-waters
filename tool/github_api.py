import logging
import os
import time
from typing import Dict, Optional

import requests

# GitHub API authentication - loaded lazily to avoid import-time crashes
github_token = os.getenv("GITHUB_API_TOKEN")

headers = {
    "Authorization": f"Bearer {github_token}",
    "Accept": "application/vnd.github.v3+json",
}

headers_v4 = {
    "Authorization": f"Bearer {github_token}",
    "Accept": "application/vnd.github.v4+json",
}

GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"


def make_github_request(
    url: str,
    method: str = "GET",
    headers: Dict = headers,
    json_data: Optional[Dict] = None,
    max_retries: int = 1,
    retry_delay: int = 2,
    timeout: int = 20,
    sleep_between_requests: int = 0,
    silent: bool = False,
) -> Optional[Dict]:
    """
    Make a HTTP request with retry logic and rate limiting handling.

    Args:
        url (str): HTTP URL
        method (str): HTTP method ("GET" or "POST")
        headers (Dict): Request headers
        json_data (Optional[Dict]): JSON payload for POST requests
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Base time to wait between retries in seconds
        timeout (int): Request timeout in seconds
        silent (bool): Whether to suppress error logging

    Returns:
        Optional[Dict]: JSON response or None if request failed
    """
    for attempt in range(max_retries):
        try:
            response = requests.request(method=method, url=url, headers=headers, json=json_data, timeout=timeout)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            time.sleep(sleep_between_requests)
            if isinstance(e, requests.exceptions.HTTPError) and (
                e.response.status_code in [429, 403] or "rate limit" in e.response.text.lower()
            ):
                if attempt == max_retries - 1:
                    if not silent:
                        logging.error(f"Failed after {max_retries} attempts due to rate limiting: {e}")
                    return None

                # Get rate limit reset time and wait
                reset_time = int(e.response.headers.get("X-RateLimit-Reset", 0))
                wait_time = max(reset_time - int(time.time()), 0) + 1
                if not silent:
                    logging.warning(f"Rate limit exceeded. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                # Handle other errors
                if not silent:
                    logging.warning(f"Request failed: {e}")
                if attempt == max_retries - 1:
                    if e.response.status_code in [
                        502,
                        504,
                    ]:  # timeout, sometimes happens when the request is too large (e.g., too many tags)
                        return 504
                    return None
                time.sleep(retry_delay * (attempt + 1))

    return None


def get_last_page_info(
    url: str, max_retries: int = 1, retry_delay: int = 2, sleep_between_requests: int = 0
) -> Optional[int]:
    """
    Get the last page number from the response headers.

    Args:
        url (str): URL to get the last page number
        max_retries (int): Maximum number
        retry_delay (int): Base time to wait between retries in seconds
        sleep_between_requests (int): Time to sleep between requests in seconds

    Returns:
        Optional[int]: Last page number or None if request failed
    """

    # We can't just use make_github_request here because we need to access the response headers
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            if "last" in response.links:
                last_page = int(response.links["last"]["url"].split("=")[-1])
            else:
                # Otherwise, the last page is the first page too
                last_page = 1
            return last_page

        except requests.exceptions.RequestException as e:
            time.sleep(sleep_between_requests)
            if attempt == max_retries - 1:
                logging.error(f"Failed after {max_retries} attempts: {e}")
                return None
            time.sleep(retry_delay * (attempt + 1))
