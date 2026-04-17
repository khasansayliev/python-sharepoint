import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests_ntlm import HttpNtlmAuth
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

LISTS: List[str] = ['Committee Members']

BLOB_BASE_PATH = "landing/sp/ucms/v1"

# ──────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────
def _serialize_datetime(obj: Any) -> str:
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _sanitize_name(name: str) -> str:
    """Normalize a SharePoint list title into a safe blob path segment."""
    return re.sub(r"[^a-z0-9_]", "_", name.lower().strip())


def save_to_blob(blob_name: str, data: Any) -> None:
    """Serialize data to JSON and save locally (Azure hook commented out)."""
    json_payload = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        default=_serialize_datetime,
    )
    with open("data.json", "w", encoding="UTF-8") as f:
        f.write(json_payload)
    # hook = WasbHook(wasb_conn_id=WASB_CONN_ID)
    # hook.load_string(
    #     container_name=BLOB_CONTAINER,
    #     blob_name=blob_name,
    #     string_data=json_payload,
    #     overwrite=True,
    # )
    logger.info("Saved %d chars to %s", len(json_payload), blob_name)


# ──────────────────────────────────────────────
# SharePoint REST Client
# ──────────────────────────────────────────────
class SharePointRESTClient:
    """
    SharePoint on-premise REST API client using NTLM authentication.
    Replaces the SOAP-based SharePointSite and SharePointList classes.
    """

    def __init__(
        self,
        site_url: str,
        username: str,
        password: str,
        verify_ssl: bool = False,
        timeout: Optional[int] = 30,
        retry: Optional[Retry] = None,
    ) -> None:
        self.site_url = site_url.rstrip("/")
        self._verify_ssl = verify_ssl
        self.timeout = timeout

        if retry is None:
            retry = Retry(
                total=5,
                read=5,
                connect=5,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504],
            )

        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._session.auth = HttpNtlmAuth(username, password)
        self._session.headers.update({
            "Accept": "application/json;odata=verbose",
            "Content-Type": "application/json;odata=verbose",
        })

    def _get(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Send a GET request and return the parsed JSON response."""
        response = self._session.get(
            url,
            params=params,
            verify=self._verify_ssl,
            timeout=self.timeout,
        )
        logger.debug("GET %s -> %s", url, response.status_code)
        response.raise_for_status()
        return response.json()

    # ── Field metadata ────────────────────────
    def _get_list_fields(self, list_name: str) -> List[Dict]:
        """Fetch field definitions for a list to identify lookup and user fields."""
        url = (
            f"{self.site_url}/_api/web/lists/getbytitle('{list_name}')/fields"
            f"?$filter=Hidden eq false and ReadOnlyField eq false"
        )
        data = self._get(url)
        return data.get("d", {}).get("results", [])

    def _build_select_expand(self, fields: List[Dict]):
        """
        Build $select and $expand query parameters from field definitions.
        Lookup and User fields are expanded to retrieve their Title subproperty.
        """
        select_parts = []
        expand_parts = []

        for field in fields:
            name = field.get("EntityPropertyName", "")
            field_type = field.get("TypeAsString", "")

            if not name:
                continue

            if field_type in ("Lookup", "LookupMulti"):
                # Expand lookup fields to get display value instead of raw ID
                select_parts.append(f"{name}/Title")
                expand_parts.append(name)
            elif field_type in ("User", "UserMulti"):
                # Expand user fields to get display name instead of claim token
                select_parts.append(f"{name}/Title")
                expand_parts.append(name)
            else:
                select_parts.append(name)

        return ",".join(select_parts), ",".join(expand_parts)

    # ── Value cleaning ────────────────────────
    @staticmethod
    def _clean_value(value: Any, field_type: str) -> Any:
        """
        Convert a raw REST API field value to a clean Python type.
        Equivalent to the SOAP-based _python_type() method.
        """
        if value is None:
            return None

        # Lookup and User fields come as dicts: {"Title": "Engineering"}
        if field_type in ("Lookup", "User") and isinstance(value, dict):
            return value.get("Title", value)

        # Multi-value Lookup and User fields come as list of dicts
        if field_type in ("LookupMulti", "UserMulti") and isinstance(value, list):
            return [v.get("Title", v) for v in value if isinstance(v, dict)]

        # Parse ISO 8601 datetime strings
        if field_type == "DateTime" and isinstance(value, str):
            try:
                return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return value

        # Convert boolean to human-readable string
        if field_type == "Boolean":
            if value is True:
                return "Yes"
            if value is False:
                return "No"
            return value

        # MultiChoice values arrive as ";#Alice;#Bob;#" — split into a list
        if field_type == "MultiChoice" and isinstance(value, str):
            return [v for v in value.split(";#") if v.strip()]

        # Cast numeric fields to float
        if field_type in ("Number", "Currency") and value != "":
            try:
                return float(value)
            except (ValueError, TypeError):
                return value

        return value

    # ── Main public method ────────────────────
    def get_list_items(
        self,
        list_name: str,
        row_limit: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all items from a SharePoint list via the REST API.
        Drop-in replacement for the SOAP-based get_list_items() method.

        - Lookup and User fields are automatically expanded
        - All field types are converted to clean Python values
        - Pagination is handled automatically (supports lists with 5000+ items)
        """
        # Fetch field metadata to build select/expand and type map
        fields = self._get_list_fields(list_name)
        field_type_map = {
            f["EntityPropertyName"]: f["TypeAsString"]
            for f in fields
            if f.get("EntityPropertyName")
        }

        # Build OData query parameters
        select, expand = self._build_select_expand(fields)

        url = f"{self.site_url}/_api/web/lists/getbytitle('{list_name}')/items"
        params: Dict[str, Any] = {}
        if select:
            params["$select"] = select
        if expand:
            params["$expand"] = expand
        if row_limit:
            params["$top"] = row_limit

        # Paginate through all results — SharePoint pages at 5000 items by default
        all_items = []
        next_url: Optional[str] = url

        while next_url:
            # First request uses explicit params; subsequent pages use the __next URL
            data = self._get(next_url, params=params if next_url == url else None)

            d = data.get("d", {})
            results = d.get("results", [])

            # Clean and collect each row
            for row in results:
                cleaned: Dict[str, Any] = {}
                for key, value in row.items():
                    if key.startswith("__"):
                        # Skip internal OData metadata fields
                        continue
                    field_type = field_type_map.get(key, "Text")
                    cleaned[key] = self._clean_value(value, field_type)
                all_items.append(cleaned)

            # Follow the next page link if present
            next_url = d.get("__next")

        logger.info("%s: %d items fetched via REST", list_name, len(all_items))
        return all_items


# ──────────────────────────────────────────────
# Main pipeline
# ──────────────────────────────────────────────
def extract_and_load() -> None:
    """Extract items from each configured SharePoint list and save to blob storage."""
    sharepoint_host = ''
    sharepoint_username = ''
    sharepoint_password = ''

    if not sharepoint_host.endswith("/apps/ucms"):
        sharepoint_host = f"{sharepoint_host}/apps/ucms"

    client = SharePointRESTClient(
        site_url=sharepoint_host,
        username=sharepoint_username,
        password=sharepoint_password,
        verify_ssl=False,
    )

    if not LISTS:
        logger.warning("LISTS is empty — nothing to extract.")
        return

    now = datetime.now()
    timestamp = now.strftime("%Y%m%d%H%M%S")
    date_parts = f"{now:%Y}/{now:%m}/{now:%d}"

    for list_title in LISTS:
        try:
            items = client.get_list_items(list_title)
            logger.info("%s: %d items fetched", list_title, len(items))

            if not items:
                continue

            safe_name = _sanitize_name(list_title)
            blob_path = f"{BLOB_BASE_PATH}/{safe_name}/{date_parts}/{safe_name}_{timestamp}.json"
            save_to_blob(blob_path, items)

        except Exception:
            logger.exception("Failed to extract list '%s'", list_title)
            raise


if __name__ == "__main__":
    extract_and_load()
