from importlib.metadata import version as get_version, PackageNotFoundError
import logging
from time import sleep

import requests

logger = logging.getLogger(__name__)
try:
    CURRENT_VERSION = get_version("pangaeapy")
except PackageNotFoundError:
    CURRENT_VERSION = None

def get_request(url, accepted_type=None, auth_token=None, timeout=10, num_retries=1):
    header = {"User-Agent": f"pangaeapy/{CURRENT_VERSION}"}
    if accepted_type is not None:
        header["Accept"] = accepted_type
    if auth_token is not None:
        header["Authorization"] = f"Bearer {auth_token}"
    response = requests.get(url, headers=header, timeout=(3.05, timeout))
    if response.status_code == 429 and num_retries > 0:
        sleep_time = int(response.headers.get("Retry-After", 30))
        logger.warning("Received too many requests error (429)...waiting %ds", sleep_time)
        sleep(sleep_time)
        response = get_request(url, accepted_type, auth_token, timeout, num_retries-1)
        logger.info("After repeating request, got status code: %d", response.status_code)
    return response

def get_xml_content(xml_root, path, namespaces=None, key=None, multiple=False):
    try:
        nodes = xml_root.findall(path, namespaces=namespaces)
    except AttributeError:
        nodes = []
    if key is not None:
        vals = [node.get(key) for node in nodes]
    else:
        vals = [node.text for node in nodes]
    if not multiple:
        try:
            vals = vals[0]
        except IndexError:
            vals = None
    return vals
