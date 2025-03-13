import logging
import re

import requests

logger = logging.getLogger("panquery")

class PanQuery:
    """Run and analyze results of PANGAEA search queries.

    Parameters
    ----------
    query : str
        The query string following the specs at www.pangaea.de.
    bbox : tuple of floats, optional
        The bounding box to define geographical search constraints
        following the GeoJSON specs -- (minlon, minlat, maxlon, maxlat).
    limit : int, default 10
        The maximum number of results returned (cannot be higher than
        500).
    offset : int, default 0
        The offset of the search results.

    Attributes
    ----------
    totalcount : int
        The number of total search results.
    error : str
        In case an error occurs this attribute holds the latest one.
    query : str
        The query provided by the user.
    result : list of dictionaries
        A list of retrieved search results.
    """
    def __init__(self, query, bbox=None, limit=10, offset=0):
        self.totalcount = 0
        self.error = None
        self.query = query
        self.result = []
        self._search(query, bbox, limit, offset)
        if self.error is not None:
            logger.error(f"ERROR: {self.error}")

    def _search(self, query, bbox, limit, offset):
        """Performs the search.

        Parameters
        ----------
        query : str
            The query string.
        bbox : tuple of floats, optional
            The bounding box.
        limit : int
            The maximum number of results returned.
        offset : int
            The offset of the search results.
        """
        params = {"q": query, "count": limit, "offset": offset}
        if bbox is not None:
            try:
                params |= {
                    "minlon": bbox[0],
                    "minlat": bbox[1],
                    "maxlon": bbox[2],
                    "maxlat": bbox[3],
                }
            except (IndexError, TypeError):
                self.error = "Request failed: Invalid bbox"
                return
        try:
            req = requests.get(
                "https://www.pangaea.de/advanced/search.php",
                params=params,
                timeout=(3.05, 10),
            )
            req.raise_for_status()
            response = req.json()
        except requests.RequestException as exc:
            self.error = f"Request failed: {exc}"
            return
        self.totalcount = response["totalCount"]
        results = response["results"]
        for i, result in enumerate(results):
            if re.search(r">\d+ datasets<", result["html"]) is not None:
                result["type"] = "collection"
            else:
                result["type"] = "member"
            result["position"] = offset + i
        self.result = results
