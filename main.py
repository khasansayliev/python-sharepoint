import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from lxml import etree
from requests_ntlm import HttpNtlmAuth
from requests_toolbelt import SSLAdapter
from urllib3.util.retry import Retry
import urllib3

from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.hooks.base import BaseHook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

LISTS: List[str] = ['BH Case Info', 'Case Management Notices', 'Case Management Plans', 'Committee Members', 'Committee Names', 'Defense Experts', 'Deponent Name - Lookup', 'Depositions', 'Document Requests - Docket', 'Expert Reports Served', 'GF Lit Plan - Document Requests Served On The Trustee', 'GF Lit Plan - Interrogatories Served By The Trustee', 'GF Lit Plan - Interrogatories Served On The Trustee', 'GF Lit Plan - Mediation', 'GF Lit Plan - Request for Admissions Served By The Trustee', 'GF Lit Plan - Request for Admissions Served On The Trustee', 'GF Lit Plan - Rule 34 Document Requests Served By The Trustee', 'GF Lit Plan - Rule 45 Document Requests Served By The Trustee', 'Initial Disclosures - Docket', 'Interrogatories - Docket', 'Key Players', 'KeyBHInterest', 'KeyRole', 'NGF Lit Plan - Stipulations', 'Non-Defendant Person or Entity Names', 'Pro Se or Company Contact Information', 'Request for Admissions - Docket', 'Rule 45 Document Requests - Docket', 'Trustees Experts', 'UserInfo']

WASB_CONN_ID = "conn_wasb_localdatalake"
SHAREPOINT_CONN_ID = "conn-ntlm-sharepoint-onprem"
BLOB_CONTAINER = "lake"
BLOB_BASE_PATH = "landing/sp/ucms/v1"

NS_SOAPENV = "http://schemas.xmlsoap.org/soap/envelope/"
NS_SHAREPOINT = "http://schemas.microsoft.com/sharepoint/soap/"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_ROWSET = "#RowsetSchema"


# ──────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────
def _post(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """POST request with logging."""
    response = session.post(url, **kwargs)
    logger.debug("POST %s -> %s", url, response.status_code)
    response.raise_for_status()
    return response


def _serialize_datetime(obj: Any) -> str:
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _sanitize_name(name: str) -> str:
    """Normalize a SharePoint list title into a safe blob path segment."""
    return re.sub(r"[^a-z0-9_]", "_", name.lower().strip())


def save_to_blob(blob_name: str, data: Any) -> None:
    """Serialize data to JSON and upload to Azure Blob Storage."""
    json_payload = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        default=_serialize_datetime,
    )
    hook = WasbHook(wasb_conn_id=WASB_CONN_ID)
    hook.load_string(
        container_name=BLOB_CONTAINER,
        blob_name=blob_name,
        string_data=json_payload,
        overwrite=True,
    )
    logger.info("Uploaded %d chars to %s/%s", len(json_payload), BLOB_CONTAINER, blob_name)


# ──────────────────────────────────────────────
# SOAP Envelope Builder
# ──────────────────────────────────────────────
class SoapEnvelope:
    """Builds a SOAP XML envelope for SharePoint web-service calls."""

    XML_DECLARATION = b'<?xml version="1.0" encoding="utf-8"?>'

    NSMAP = {
        "SOAP-ENV": NS_SOAPENV,
        "ns0": NS_SOAPENV,
        "ns1": NS_SHAREPOINT,
        "xsi": NS_XSI,
    }

    def __init__(self, command: str) -> None:
        self.envelope = etree.Element(f"{{{NS_SOAPENV}}}Envelope", nsmap=self.NSMAP)
        body = etree.SubElement(self.envelope, f"{{{NS_SOAPENV}}}Body")
        self._command = etree.SubElement(body, f"{{{NS_SHAREPOINT}}}{command}")
        self._batch: Optional[etree._Element] = None

    # ── Parameter helpers ─────────────────────
    def add_parameter(self, parameter: str, value: Optional[str] = None) -> None:
        sub = etree.SubElement(self._command, f"{{{NS_SHAREPOINT}}}{parameter}")
        if value is not None:
            sub.text = value

    def add_actions(self, data: List[Dict[str, str]], kind: str) -> None:
        """Build <Batch> element for UpdateListItems."""
        if self._batch is None:
            updates = etree.SubElement(self._command, f"{{{NS_SHAREPOINT}}}updates")
            self._batch = etree.SubElement(updates, "Batch", OnError="Return", ListVersion="1")

        if kind == "Delete":
            for index, _id in enumerate(data, 1):
                method = etree.SubElement(self._batch, "Method", ID=str(index), Cmd=kind)
                field = etree.SubElement(method, "Field", Name="ID")
                field.text = str(_id)
        else:
            for index, row in enumerate(data, 1):
                method = etree.SubElement(self._batch, "Method", ID=str(index), Cmd=kind)
                for key, value in row.items():
                    field = etree.SubElement(method, "Field", Name=key)
                    field.text = str(value)

    def add_query(self, pyquery: Dict) -> None:
        query_wrapper = etree.SubElement(self._command, f"{{{NS_SHAREPOINT}}}query")
        query = etree.SubElement(query_wrapper, "Query")

        if "OrderBy" in pyquery:
            order = etree.SubElement(query, "OrderBy")
            for field in pyquery["OrderBy"]:
                if isinstance(field, tuple):
                    attrs = {"Name": field[0]}
                    if field[1] == "DESCENDING":
                        attrs["Ascending"] = "FALSE"
                    etree.SubElement(order, "FieldRef", **attrs)
                else:
                    etree.SubElement(order, "FieldRef", Name=field)

        if "GroupBy" in pyquery:
            group = etree.SubElement(query, "GroupBy")
            for field in pyquery["GroupBy"]:
                etree.SubElement(group, "FieldRef", Name=field)

        if "Where" in pyquery:
            query.append(pyquery["Where"])

    # ── Serialization ─────────────────────────
    def to_bytes(self) -> bytes:
        return self.XML_DECLARATION + etree.tostring(self.envelope)

    def __repr__(self) -> str:
        return self.to_bytes().decode("utf-8")

    def __str__(self) -> str:
        return (self.XML_DECLARATION + etree.tostring(self.envelope, pretty_print=True)).decode("utf-8")


# ──────────────────────────────────────────────
# SharePoint List
# ──────────────────────────────────────────────
class SharePointList:
    """Represents a single SharePoint list and provides CRUD operations."""

    DATE_PATTERN = re.compile(r"\d+-\d+-\d+ \d+:\d+:\d+")
    HTML_TAG_PATTERN = re.compile(r"<[^>]+>")

    def __init__(
        self,
        session: requests.Session,
        list_name: str,
        url_builder: Callable[[str], str],
        verify_ssl: bool,
        users: Optional[Dict],
        huge_tree: bool,
        timeout: Optional[int],
        exclude_hidden_fields: bool = False,
    ) -> None:
        self._session = session
        self.list_name = list_name
        self._url = url_builder
        self._verify_ssl = verify_ssl
        self.users = users
        self.huge_tree = huge_tree
        self.timeout = timeout

        # Metadata populated by _fetch_list_metadata()
        self.fields: List[Dict[str, str]] = []
        self.regional_settings: Dict[str, str] = {}
        self.server_settings: Dict[str, str] = {}
        self._fetch_list_metadata()

        if exclude_hidden_fields:
            self.fields = [f for f in self.fields if f.get("Hidden", "FALSE") == "FALSE"]

        # FIX 4 (indentation): these were at column 0, outside the class
        self._sp_cols = {f["Name"]: {"name": f["StaticName"], "type": f["Type"]} for f in self.fields}
        self._disp_cols = {f["DisplayName"]: {"name": f["StaticName"], "type": f["Type"]} for f in self.fields}

        # Allow Title to be used as a display column
        if "Title" in self._sp_cols:
            title_info = self._sp_cols["Title"]
            self._disp_cols[title_info["name"]] = {"name": "Title", "type": title_info["type"]}

        self.last_request: Optional[str] = None

    # ── Private helpers ───────────────────────
    def _headers(self, soapaction: str) -> Dict[str, str]:
        return {
            "Content-Type": "text/xml; charset=UTF-8",
            "SOAPAction": f"{NS_SHAREPOINT}{soapaction}",
        }

    def _send_soap(self, action: str, soap: SoapEnvelope) -> etree._Element:
        """Send a SOAP request, parse and return the XML envelope."""
        self.last_request = str(soap)
        response = _post(
            self._session,
            url=self._url("Lists"),
            headers=self._headers(action),
            data=soap.to_bytes(),
            verify=self._verify_ssl,
            timeout=self.timeout,
        )
        return etree.fromstring(
            response.text.encode("utf-8"),
            parser=etree.XMLParser(huge_tree=self.huge_tree, recover=True),
        )

    @staticmethod
    def _parse_ows_row(row: etree._Element) -> Dict[str, str]:
        """Parse a single <z:row> element, stripping 'ows_' prefix."""
        return {key[4:]: value for key, value in row.items() if key.startswith("ows_")}

    # ── Type conversion ───────────────────────
    @staticmethod
    def _strip_id_hash(value: str) -> Optional[str]:
        """Strip SharePoint 'id;#value' prefix and return only the value part.

        Handles all patterns:
          "116;#Some Text"  → "Some Text"
          "116;#"           → None
          ";#Some Text"     → "Some Text"
          "plain text"      → "plain text"  (no change)
        """
        if ";#" in value:
            _, _, after = value.partition(";#")
            return after.strip() if after.strip() else None
        return value

    def _python_type(self, key: str, value: Any) -> Any:
        """Convert SharePoint internal value to a clean Python type."""

        # Universal null guard — must be first, before any type checks.
        # Empty string from ANY field type (Text, Note, Lookup, etc.) → None
        if value is None or str(value).strip() == "":
            return None

        try:
            field_type = self._sp_cols[key]["type"]

            # Numeric fields
            if field_type in ("Number", "Currency"):
                return float(value)

            # DateTime fields
            elif field_type == "DateTime":
                match = self.DATE_PATTERN.search(value)
                if match:
                    value = match.group(0)
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

            # Boolean fields
            elif field_type == "Boolean":
                return {"1": "Yes", "0": "No"}.get(str(value).strip(), None)

            # User and UserMulti fields
            elif field_type in ("User", "UserMulti"):
                if self.users and value in self.users["sp"]:
                    return self.users["sp"][value]
                elif ";#" in value:
                    parts = value.split(";#")
                    users = [parts[i] for i in range(1, len(parts), 2) if parts[i].strip()]
                    return users if len(users) > 1 else users[0] if users else None
                return value if value.strip() else None

            # Lookup and LookupMulti fields — strip leading "id;#" prefix
            elif field_type in ("Lookup", "LookupMulti"):
                if ";#" not in value:
                    return value.strip() if value.strip() else None
                parts = value.split(";#")
                names = [parts[i] for i in range(1, len(parts), 2) if parts[i].strip()]
                # e.g. "116;#" → parts = ["116", ""] → names = [] → None
                if not names:
                    return None
                return names if len(names) > 1 else names[0]

            # MultiChoice fields — ";#Alice;#Bob;#" -> ["Alice", "Bob"]
            elif field_type == "MultiChoice":
                values = [v for v in value.split(";#") if v.strip()]
                return values if values else None

            # Note (rich text / multiline) — strip HTML tags, NOT split on ";#"
            # "<div></div>" → None, "<div>Some text</div>" → "Some text"
            elif field_type == "Note":
                clean = self.HTML_TAG_PATTERN.sub("", value).strip()
                return clean if clean else None

            # Calculated fields — "resulttype;#actualvalue"
            elif field_type == "Calculated":
                if ";#" not in value:
                    return value
                result_type, result_value = value.split(";#", 1)
                result_type = result_type.lower()
                if not result_value.strip():
                    return None
                if result_type == "float":
                    return float(result_value)
                elif result_type == "datetime":
                    match = self.DATE_PATTERN.search(result_value)
                    if match:
                        result_value = match.group(0)
                    return datetime.strptime(result_value, "%Y-%m-%d %H:%M:%S")
                elif result_type == "boolean":
                    return {"1": "Yes", "0": "No"}.get(result_value, None)
                else:
                    return result_value

            # ── Universal fallback for ALL other field types ──────────────────
            # (Text, Choice, URL, Counter, Integer, Attachments, etc.)
            # If SharePoint somehow left an "id;#value" pattern, strip it.
            cleaned = self._strip_id_hash(str(value))
            return cleaned if cleaned else None

        except (AttributeError, ValueError):
            # If conversion failed but value contains id;#, still clean it up.
            cleaned = self._strip_id_hash(str(value))
            return cleaned if cleaned else None

    def _sp_type(self, key: str, value: Any) -> Any:
        """Convert Python value to SharePoint internal type."""
        try:
            field_type = self._disp_cols[key]["type"]

            if field_type in ("Number", "Currency"):
                return value
            elif field_type == "DateTime":
                return value.strftime("%Y-%m-%d %H:%M:%S")
            elif field_type == "Boolean":
                if value == "Yes":
                    return "1"
                elif value == "No":
                    return "0"
                raise ValueError(f"{value} is not a valid Boolean — expected 'Yes' or 'No'")
            elif self.users and field_type == "User":
                return self.users["py"][key]
            return value
        except AttributeError:
            return value

    def _convert_to_internal(self, data: List[Dict]) -> List[Dict]:
        """Convert display column names → internal SharePoint column names."""
        new_data = []
        for row in data:
            new_row = {}
            for key, value in row.items():
                if key not in self._disp_cols:
                    raise KeyError(f"'{key}' is not a column in list '{self.list_name}'")
                new_row[self._disp_cols[key]["name"]] = self._sp_type(key, value)
            new_data.append(new_row)
        return new_data

    def _convert_to_display(self, data: List[Dict]) -> None:
        """In-place conversion: internal column names → display names."""
        for row in data:
            for key in list(row.keys()):
                if key not in self._sp_cols:
                    continue
                row[self._sp_cols[key]["name"]] = self._python_type(key, row.pop(key))

    # ── Metadata ──────────────────────────────
    @staticmethod
    def _parse_list_envelope(
        envelope: etree._Element,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, str], Dict[str, str]]:
        """Extract fields, regional settings, and server settings from GetList response."""
        ns = {"re": "http://exslt.org/regular-expressions"}
        _list = envelope[0][0][0][0]

        fields = [
            dict(row.items())
            for row in _list.xpath("//*[re:test(local-name(), '.*Fields.*')]", namespaces=ns)[0]
        ]
        regional_settings = {
            el.tag.strip(f"{{{NS_SHAREPOINT}}}"): el.text
            for el in _list.xpath("//*[re:test(local-name(), '.*RegionalSettings.*')]", namespaces=ns)[0]
        }
        server_settings = {
            el.tag.strip(f"{{{NS_SHAREPOINT}}}"): el.text
            for el in _list.xpath("//*[re:test(local-name(), '.*ServerSettings.*')]", namespaces=ns)[0]
        }
        return fields, regional_settings, server_settings

    def _fetch_list_metadata(self) -> None:
        """Fetch and store list schema (fields, regional/server settings)."""
        soap = SoapEnvelope("GetList")
        soap.add_parameter("listName", self.list_name)
        envelope = self._send_soap("GetList", soap)

        fields, regional, server = self._parse_list_envelope(envelope)
        self.fields.extend(fields)
        self.regional_settings.update(regional)
        self.server_settings.update(server)

    # ── Public API ────────────────────────────
    # FIX 4 (indentation): get_list_items was at column 0, outside the class
    def get_list_items(self, row_limit: int = 0) -> List[Dict[str, Any]]:
        """Fetch all items from the SharePoint list."""
        soap = SoapEnvelope("GetListItems")
        soap.add_parameter("listName", self.list_name)
        soap.add_parameter("rowLimit", str(row_limit))

        # Empty <viewFields><ViewFields/></viewFields> = return ALL columns
        # Without this SharePoint only returns the default view's columns
        v = etree.SubElement(soap._command, f"{{{NS_SHAREPOINT}}}viewFields")
        etree.SubElement(v, "ViewFields")

        envelope = self._send_soap("GetListItems", soap)
        listitems = envelope[0][0][0][0][0]

        data = [self._parse_ows_row(row) for row in listitems]
        self._convert_to_display(data)
        return data


# ──────────────────────────────────────────────
# SharePoint Site
# ──────────────────────────────────────────────
class SharePointSite:
    """Connects to a SharePoint site and provides access to lists and user info."""

    SERVICES = {
        "Alerts": "/_vti_bin/Alerts.asmx",
        "Authentication": "/_vti_bin/Authentication.asmx",
        "Copy": "/_vti_bin/Copy.asmx",
        "Dws": "/_vti_bin/Dws.asmx",
        "Forms": "/_vti_bin/Forms.asmx",
        "Imaging": "/_vti_bin/Imaging.asmx",
        "DspSts": "/_vti_bin/DspSts.asmx",
        "Lists": "/_vti_bin/lists.asmx",
        "Meetings": "/_vti_bin/Meetings.asmx",
        "People": "/_vti_bin/People.asmx",
        "Permissions": "/_vti_bin/Permissions.asmx",
        "SiteData": "/_vti_bin/SiteData.asmx",
        "Sites": "/_vti_bin/Sites.asmx",
        "Search": "/_vti_bin/Search.asmx",
        "UserGroup": "/_vti_bin/usergroup.asmx",
        "Versions": "/_vti_bin/Versions.asmx",
        "Views": "/_vti_bin/Views.asmx",
        "WebPartPages": "/_vti_bin/WebPartPages.asmx",
        "Webs": "/_vti_bin/Webs.asmx",
    }

    _USER_NAME_KEYS = ("ImnName", "Name", "Title", "UserName")

    def __init__(
        self,
        site_url: str,
        auth: Optional[Any] = None,
        authcookie: Optional[requests.cookies.RequestsCookieJar] = None,
        verify_ssl: bool = True,
        ssl_version: Optional[float] = None,
        huge_tree: bool = False,
        timeout: Optional[int] = None,
        retry: Optional[Retry] = None,
    ) -> None:
        self.site_url = site_url
        self._verify_ssl = verify_ssl
        self.huge_tree = huge_tree
        self.timeout = timeout
        self.last_request: Optional[str] = None

        if retry is None:
            retry = Retry(
                total=5, read=5, connect=5,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504],
            )

        self._session = requests.Session()
        http_adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        https_adapter = SSLAdapter(ssl_version, max_retries=retry) if ssl_version else http_adapter

        self._session.mount("https://", https_adapter)
        self._session.mount("http://", http_adapter)

        if authcookie is not None:
            self._session.cookies = authcookie
        else:
            self._session.auth = auth

        self.site_info = self._fetch_site_info()
        self.users = self._fetch_users()

    # ── Private helpers ───────────────────────
    def _url(self, service: str) -> str:
        return f"{self.site_url}{self.SERVICES[service]}"

    def _headers(self, soap_action: str) -> Dict[str, str]:
        return {
            "Content-Type": "text/xml; charset=UTF-8",
            "SOAPAction": f"{NS_SHAREPOINT}{soap_action}",
        }

    def _fetch_site_info(self) -> str:
        soap = SoapEnvelope("GetSite")
        soap.add_parameter("SiteUrl", self.site_url)
        self.last_request = str(soap)

        response = _post(
            self._session,
            url=self._url("Sites"),
            headers=self._headers("GetSite"),
            data=soap.to_bytes(),
            verify=self._verify_ssl,
            timeout=self.timeout,
        )
        envelope = etree.fromstring(
            response.text.encode("utf-8"),
            # FIX 4 (indentation): parser= line was at column 0
            parser=etree.XMLParser(huge_tree=self.huge_tree, recover=True),
        )
        return envelope[0][0][0].text

    def _fetch_users(self, rowlimit: int = 0) -> Optional[Dict[str, Dict[str, str]]]:
        """Fetch UserInfo list and build bidirectional lookup maps."""
        soap = SoapEnvelope("GetListItems")
        soap.add_parameter("listName", "UserInfo")
        soap.add_parameter("rowLimit", str(rowlimit))
        self.last_request = str(soap)

        response = _post(
            self._session,
            url=self._url("Lists"),
            headers=self._headers("GetListItems"),
            data=soap.to_bytes(),
            verify=self._verify_ssl,
            timeout=self.timeout,
        )

        try:
            envelope = etree.fromstring(
                response.text.encode("utf-8"),
                parser=etree.XMLParser(huge_tree=self.huge_tree, recover=True),
            )
        except Exception as exc:
            raise requests.ConnectionError(
                f"GetUsers response failed to parse: {exc}"
            ) from exc

        listitems = envelope[0][0][0][0][0]

        data = []
        for row in listitems:
            data.append({key[4:]: value for key, value in row.items() if key.startswith("ows_")})

        if data:
            logger.debug("User row sample keys: %s", list(data[0].keys()))

        user_map_py: Dict[str, str] = {}
        user_map_sp: Dict[str, str] = {}
        for user_row in data:
            user_id = user_row.get("ID")
            if not user_id:
                continue

            user_name = None
            for key in self._USER_NAME_KEYS:
                if key in user_row and user_row[key]:
                    user_name = user_row[key]
                    break

            if not user_name:
                logger.warning("User ID=%s has no recognizable name field, skipping. Keys: %s",
                               user_id, list(user_row.keys()))
                continue

            sp_key = f"{user_id};#{user_name}"
            user_map_py[user_name] = sp_key
            user_map_sp[sp_key] = user_name

        logger.info("Loaded %d users from SharePoint UserInfo list", len(user_map_py))
        return {"py": user_map_py, "sp": user_map_sp}

    # ── Public API ────────────────────────────
    def get_list(self, list_name: str, exclude_hidden_fields: bool = False) -> SharePointList:
        """Return a SharePointList object for the given list name."""
        return SharePointList(
            self._session,
            list_name,
            self._url,
            self._verify_ssl,
            self.users,
            self.huge_tree,
            self.timeout,
            exclude_hidden_fields=exclude_hidden_fields,
        )


# ──────────────────────────────────────────────
# Airflow DAG
# ──────────────────────────────────────────────
@dag(
    dag_id="sharepoint_to_azure_blob_pipeline",
    default_args={
        "owner": "data_engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    schedule="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["sharepoint", "azure_blob"],
)
def sharepoint_extraction_dag():

    @task()
    def extract_and_load() -> None:
        conn = BaseHook.get_connection(SHAREPOINT_CONN_ID)

        host = conn.host
        if not host.endswith("/apps/ucms"):
            host = f"{conn.host}/apps/ucms"
        site = SharePointSite(
            host,
            auth=HttpNtlmAuth(conn.login, conn.password),
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
                sp_list = site.get_list(list_title)
                items = sp_list.get_list_items()

                logger.info("%s: %d items fetched", list_title, len(items))

                if not items:
                    continue

                safe_name = _sanitize_name(list_title)
                blob_path = f"{BLOB_BASE_PATH}/{safe_name}/{date_parts}/{safe_name}_{timestamp}.json"
                save_to_blob(blob_path, items)

            except Exception:
                logger.exception("Failed to extract list '%s'", list_title)
                raise

    extract_and_load()


sharepoint_dag = sharepoint_extraction_dag()
