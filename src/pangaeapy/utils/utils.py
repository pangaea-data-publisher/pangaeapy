import re
import xml.etree.ElementTree as ET

def get_id(node: ET.Element, cast: bool = False) -> int | str | None:
    return (int if cast else str)(re.search(r"(\d+)$", id_string).group(1)) if (id_string := node.get("id")) is not None else None
