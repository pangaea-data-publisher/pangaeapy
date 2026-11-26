import json
from pathlib import Path

from requests import Session

from .cache import read_cache, write_cache
from .retry import HTTP_ADAPTER

def load_metadata(
    dataset_id: int,
    enable_cache: bool,
    cachedir: Path | None,
    cache_expiry_days: int | None,
) -> tuple[str, str | None]:
    xml = read_cache(enable_cache, cachedir, cache_expiry_days, f"{dataset_id}.xml")
    citation = read_cache(enable_cache, cachedir, cache_expiry_days, f"{dataset_id}.txt")
    if xml is None or (citation is None and not xml.startswith("<md:DeletedMetaData")):
        with Session() as session:
            session.mount("https://", HTTP_ADAPTER)
            if xml is None:
                resp = session.get(
                    "https://ws.pangaea.de/es/pangaea-all/panmd/_search",
                    headers={"Accept": "application/json"},
                    json={
                        "query": {"ids": {"values": [dataset_id]}},
                        "size": 1,
                        "_source": "xml",
                    },
                    timeout=(3.05, 10),
                )
                resp.raise_for_status()
                xml = resp.json()["hits"]["hits"][0]["_source"]["xml"]
                write_cache(enable_cache, cachedir, f"{dataset_id}.xml", xml)
            if citation is None and not xml.startswith("<md:DeletedMetaData"):
                resp = session.get(
                    f"https://doi.pangaea.de/10.1594/PANGAEA.{dataset_id}",
                    headers={"Accept": "text/x-bibliography"},
                    timeout=(3.05, 10),
                )
                resp.raise_for_status()
                citation = resp.text
                write_cache(enable_cache, cachedir, f"{dataset_id}.txt", citation)
    return xml, citation

def load_terms(
    terms: list[str],
    enable_cache: bool,
    cachedir: Path | None,
    cache_expiry_days: int | None,
) -> dict[str, dict[str, str | list[str]]]:
    terms_info = {}
    missing = []
    for term in terms:
        try:
            terms_info[term] = json.loads(read_cache(enable_cache, cachedir, cache_expiry_days, f"{term}.json"))
        except TypeError:
            missing.append(term)
    if missing:
        with Session() as session:
            session.mount("https://", HTTP_ADAPTER)
            resp = session.get(
                "https://ws.pangaea.de/es/pangaea-terms/term/_search",
                headers={"Accept": "application/json"},
                json={
                    "query": {"ids": {"values": missing}},
                    "size": len(missing),
                    "_source": ["main_topics", "topics"],
                },
                timeout=(3.05, 10),
            )
        resp.raise_for_status()
        for term in resp.json()["hits"]["hits"]:
            write_cache(enable_cache, cachedir, f"{term['_id']}.json", json.dumps(term["_source"]))
            terms_info[term["_id"]] = term["_source"]
    return terms_info

def load_data(
    dataset_id: int,
    enable_cache: bool,
    cachedir: Path | None,
    cache_expiry_days: int | None,
    auth_token: str | None,
) -> str:
    data = read_cache(enable_cache, cachedir, cache_expiry_days, f"{dataset_id}.csv")
    if data is None:
        with Session() as session:
            session.mount("https://", HTTP_ADAPTER)
            resp = session.get(
                f"https://doi.pangaea.de/10.1594/PANGAEA.{dataset_id}",
                headers={
                    "Accept": "text/tab-separated-values",
                    "Authorization": f"Bearer {auth_token}" if auth_token is not None else None,
                },
                timeout=(3.05, 10),
            )
        resp.raise_for_status()
        data = resp.text
        write_cache(enable_cache, cachedir, f"{dataset_id}.csv", data)
    return data[data.index("\n*/\n")+4:]
