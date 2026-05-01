from __future__ import annotations

import atexit
import contextlib
import datetime as dt
import fcntl
import json
import os
import re
import threading
import time
from types import TracebackType
from typing import Any, Optional

import requests
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential, ClientSecretCredential
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS
from requests.models import Response

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.shortcuts import ShortcutClient

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)

livysession_credentials: FabricSparkCredentials

DEFAULT_POLL_WAIT = 10
DEFAULT_POLL_STATEMENT_WAIT = 5
AZURE_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
FABRIC_NOTEBOOK_CREDENTIAL_SCOPE = "pbi"
accessToken: AccessToken = None

# Global lock to ensure thread-safe session creation/reuse
_session_lock = threading.Lock()

# Global lock to ensure thread-safe token refresh
_token_lock = threading.Lock()

# Process-level cache for lakehouse properties (avoids repeated API calls per connection open)
_lakehouse_props_cache: dict[tuple[str, str, str], dict] = {}
_lakehouse_props_lock = threading.Lock()


def read_session_id_from_file(file_path: str) -> Optional[str]:
    """Read session ID from file if it exists and contains a valid ID.

    Parameters
    ----------
    file_path : str
        Path to the session ID file.

    Returns
    -------
    Optional[str]
        The session ID if file exists and contains one, None otherwise.
    """
    try:
        if not os.path.exists(file_path):
            logger.debug(f"Session ID file does not exist: {file_path}")
            return None

        with open(file_path, "r") as f:
            session_id = f.read().strip()
            if session_id:
                logger.debug(f"Read session ID from file: {session_id}")
                return session_id
            else:
                logger.debug(f"Session ID file exists but is empty: {file_path}")
                return None
    except Exception as ex:
        logger.debug(f"Error reading session ID file: {ex}")
        return None


def write_session_id_to_file(file_path: str, session_id: str) -> bool:
    """Write session ID to file.

    Parameters
    ----------
    file_path : str
        Path to the session ID file.
    session_id : str
        The session ID to write.

    Returns
    -------
    bool
        True if successful, False otherwise.
    """
    try:
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        with open(file_path, "w") as f:
            f.write(session_id)
        logger.debug(f"Wrote session ID to file: {session_id} -> {file_path}")
        return True
    except Exception as ex:
        logger.warning(f"Error writing session ID to file: {ex}")
        return False


@contextlib.contextmanager
def cross_process_session_lock(session_file_path: str, timeout: int = 600):
    """Cross-process file lock for Livy session creation.

    When multiple dbt processes start simultaneously (e.g. parallel sub-agent
    commands), only the first process should create a Spark session.  Other
    processes block on this lock until the first one writes the session ID
    file, then reuse that session.

    Uses fcntl.flock (advisory lock) on a .lock file next to the session ID
    file.  The lock is automatically released when the context exits or the
    process dies.

    Note: upstream's ``reuse_session`` flag was designed for **sequential**
    session reuse (Run 1 exits → Run 2 starts and reuses).  This lock adds
    **parallel** session sharing across concurrent processes.  When
    ``reuse_session=False`` (the default), the atexit handler will still
    attempt to delete the Spark session — if Process 1 exits before
    Processes 2-7 finish, those processes may lose the session.  For
    parallel workloads, set ``reuse_session=True`` and let Fabric's idle
    timeout handle cleanup, or accept that short-lived parallel commands
    may occasionally need to recreate the session.

    Parameters
    ----------
    session_file_path : str
        Path to the session ID file.  The lock file is ``<path>.lock``.
    timeout : int
        Max seconds to wait for the lock before giving up (default 600s /
        10 minutes — enough for Spark cluster cold start).
    """
    lock_path = session_file_path + ".lock"
    lock_dir = os.path.dirname(lock_path)
    if lock_dir and not os.path.exists(lock_dir):
        os.makedirs(lock_dir, exist_ok=True)

    lock_fd = None
    try:
        lock_fd = open(lock_path, "w")
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.debug(f"Acquired cross-process session lock: {lock_path}")
                break
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Timed out waiting {timeout}s for session lock {lock_path}. "
                        f"Another dbt process may be stuck creating a Spark session. "
                        f"Delete {lock_path} to unblock."
                    )
                logger.debug(
                    f"Session lock held by another process, waiting... ({lock_path})"
                )
                time.sleep(2)
        yield
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
            except Exception:
                pass



def is_token_refresh_necessary(unixTimestamp: int) -> bool:
    # Convert to datetime object
    dt_object = dt.datetime.fromtimestamp(unixTimestamp)
    # Convert to local time
    local_time = time.localtime(time.time())

    # Calculate difference
    difference = dt_object - dt.datetime.fromtimestamp(time.mktime(local_time))
    if int(difference.total_seconds() / 60) < 5:
        logger.debug(f"Token Refresh necessary in {int(difference.total_seconds() / 60)}")
        return True
    else:
        return False


def get_cli_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the CLI credentials

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: FabricConnectionManager
        The credentials.

    Returns
    -------
    out : AccessToken
        Access token.
    """
    _ = credentials
    accessToken = AzureCliCredential().get_token(AZURE_CREDENTIAL_SCOPE)
    return accessToken


def get_sp_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the SP credentials.

    Parameters
    ----------
    credentials : FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    accessToken = ClientSecretCredential(
        str(credentials.tenant_id), str(credentials.client_id), str(credentials.client_secret)
    ).get_token(AZURE_CREDENTIAL_SCOPE)
    return accessToken


def get_default_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the SP Default Credentials.

    Parameters
    ----------
    credentials : FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    expires_on = 1845972874

    # Create an AccessToken instance
    accessToken = AccessToken(token=credentials.accessToken, expires_on=expires_on)
    return accessToken


def get_env_access_token() -> AccessToken:
    """
    Get an access token from the FABRIC_LAKEHOUSE_ACCESS_TOKEN environment variable.

    Used with authentication=env_oauth_access_token. The token is read fresh
    on every call (no caching) — the user is responsible for keeping the env
    var up to date before each dbt command.

    Returns
    -------
    out : AccessToken
        The access token.

    Raises
    ------
    DbtRuntimeError
        If the FABRIC_LAKEHOUSE_ACCESS_TOKEN environment variable is not set.
    """
    token = os.environ.get("FABRIC_LAKEHOUSE_ACCESS_TOKEN")
    if not token:
        raise DbtRuntimeError(
            "authentication is set to 'env_oauth_access_token' but the "
            "FABRIC_LAKEHOUSE_ACCESS_TOKEN environment variable is not set."
        )
    # expires_on=0 since we always read fresh and never use the cached value
    return AccessToken(token=token, expires_on=0)


def get_vdstudio_oauth_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an access token from vd-studio's local /az_token endpoint.

    vd-studio runs a local HTTP server that handles all OAuth refresh logic.
    This method fetches a fresh token on every call — same pattern as SP auth
    refresh, but calling the local endpoint instead of Azure AD directly.

    Parameters
    ----------
    credentials : FabricSparkCredentials
        Credentials (must have vdstudio_oauth_endpoint_url set).

    Returns
    -------
    out : AccessToken
        The access token.
    """
    endpoint_url = (
        os.environ.get("VD_STUDIO_TOKEN_URL")
        or credentials.vdstudio_oauth_endpoint_url
    )
    if not endpoint_url:
        raise DbtRuntimeError(
            "authentication is set to 'vdstudio_oauth' but neither "
            "VD_STUDIO_TOKEN_URL env variable nor "
            "vdstudio_oauth_endpoint_url in profile is configured."
        )

    user_id = os.environ.get("VD_STUDIO_USER_ID")
    if not user_id:
        raise DbtRuntimeError(
            "authentication is set to 'vdstudio_oauth' but the "
            "VD_STUDIO_USER_ID environment variable is not set."
        )

    try:
        resp = requests.get(
            endpoint_url,
            params={"scope": AZURE_CREDENTIAL_SCOPE, "user_id": user_id},
            timeout=10,
        )
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        raise DbtRuntimeError(
            f"vdstudio_oauth: could not reach token endpoint at {endpoint_url}. "
            "Is vd-studio running?"
        ) from e

    if resp.status_code == 401:
        raise DbtRuntimeError(
            "vdstudio_oauth: token refresh failed on vd-studio side. "
            "Please re-login in vd-studio."
        )
    if resp.status_code == 403:
        raise DbtRuntimeError(
            "vdstudio_oauth: request rejected by token endpoint. "
            "Must be called from localhost."
        )
    resp.raise_for_status()

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise DbtRuntimeError(
            "vdstudio_oauth: token server response missing access_token field"
        )

    expires_on = int(time.time()) + data.get("expires_in", 3600)
    return AccessToken(token=token, expires_on=expires_on)


def get_fabric_notebook_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using notebookutils.

    Works in both Fabric PySpark and Python notebooks.

    Note: notebookutils is only available in Fabric notebook runtime environments.
    It is not installable via pip and will not resolve in local development.

    Parameters
    ----------
    credentials : FabricSparkCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    import base64  # noqa: F401

    import notebookutils  # type: ignore  # noqa: F401 - only available in Fabric runtime

    _ = credentials
    aad_token = notebookutils.credentials.getToken(FABRIC_NOTEBOOK_CREDENTIAL_SCOPE)
    expires_on = json.loads(base64.b64decode(aad_token.split(".")[1] + "=="))["exp"]

    now = time.time()
    remaining_seconds = expires_on - now
    remaining_minutes = remaining_seconds / 60
    logger.debug(
        f"Token expiry: {dt.datetime.fromtimestamp(expires_on).isoformat()}, "
        f"Current time: {dt.datetime.fromtimestamp(now).isoformat()}, "
        f"Remaining: {remaining_minutes:.1f} minutes"
    )

    accessToken = AccessToken(token=aad_token, expires_on=expires_on)
    return accessToken


def get_headers(credentials: FabricSparkCredentials, tokenPrint: bool = False) -> dict[str, str]:
    """Get HTTP headers for Livy requests.

    For local mode, no authentication is required.
    For Fabric mode, Azure authentication is used.
    """
    if credentials.is_local_mode:
        # Local Livy doesn't require authentication
        return {"Content-Type": "application/json"}

    global accessToken
    # env_oauth_access_token: always read fresh from env var, never cache
    if credentials.authentication and credentials.authentication.lower() == "env_oauth_access_token":
        if accessToken is None:
            logger.info("Using env_oauth_access_token auth (reading FABRIC_LAKEHOUSE_ACCESS_TOKEN)")
        accessToken = get_env_access_token()
    else:
        with _token_lock:
            if accessToken is None or is_token_refresh_necessary(accessToken.expires_on):
                if credentials.authentication and credentials.authentication.lower() == "cli":
                    logger.info("Using CLI auth")
                    accessToken = get_cli_access_token(credentials)
                elif credentials.authentication and credentials.authentication.lower() == "int_tests":
                    logger.info("Using int_tests auth")
                    accessToken = get_default_access_token(credentials)
                elif (
                    credentials.authentication
                    and credentials.authentication.lower() == "fabric_notebook"
                ):
                    logger.info("Using Fabric Notebook auth")
                    accessToken = get_fabric_notebook_access_token(credentials)
                elif (
                    credentials.authentication
                    and credentials.authentication.lower() == "vdstudio_oauth"
                ):
                    logger.info("Using vdstudio_oauth auth")
                    accessToken = get_vdstudio_oauth_access_token(credentials)
                else:
                    logger.info("Using SPN auth")
                    accessToken = get_sp_access_token(credentials)

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {accessToken.token}"}
    if tokenPrint:
        logger.debug(f"token is : {accessToken.token}")

    return headers


def _parse_retry_after(response: requests.Response) -> float:
    """Extract wait time from Retry-After header or 429 response body.

    Falls back to 0 if no hint is found.
    """
    header = response.headers.get("Retry-After", "")
    if header:
        try:
            return float(header)
        except ValueError:
            pass
    # Fabric 429 body sometimes includes a retry-after timestamp in the message
    try:
        body = response.json()
        msg = body.get("message", "")
        # Fabric 429 body includes a timestamp like "...until: 4/17/2026 12:22:35 PM (UTC)"
        if "until:" in msg:
            ts_str = msg.split("until:")[1].strip().rstrip(")")
            ts_str = ts_str.replace("(UTC", "").strip()
            target = dt.datetime.strptime(ts_str, "%m/%d/%Y %I:%M:%S %p")
            delta = (target - dt.datetime.utcnow()).total_seconds()
            return max(delta, 0)
    except Exception:
        pass
    return 0


def get_lakehouse_properties(credentials: FabricSparkCredentials) -> dict:
    """Fetch lakehouse properties from the Fabric REST API.

    Calls GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId} and returns
    the ``properties`` dict from the response. The presence of ``defaultSchema``
    in the returned dict indicates a schema-enabled lakehouse.

    Results are cached per process so parallel calls don't stampede
    the API on every connection open.

    Returns an empty dict for local mode (no Fabric API available).
    """
    if credentials.is_local_mode:
        return {}

    cache_key = (credentials.endpoint, credentials.workspaceid, credentials.lakehouseid)

    with _lakehouse_props_lock:
        if cache_key in _lakehouse_props_cache:
            logger.debug("Lakehouse properties served from cache")
            return _lakehouse_props_cache[cache_key]

    headers = get_headers(credentials)
    url = f"{credentials.endpoint}/workspaces/{credentials.workspaceid}/lakehouses/{credentials.lakehouseid}"

    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 429:
                retry_after = _parse_retry_after(response)
                wait = max(retry_after, 2**attempt * 2)  # at least 2, 4, 8, 16, 32s
                logger.debug(
                    f"Lakehouse properties API returned 429, "
                    f"retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            properties = response.json().get("properties", {})
            logger.debug(f"Lakehouse properties: {properties}")

            with _lakehouse_props_lock:
                _lakehouse_props_cache[cache_key] = properties

            return properties
        except requests.exceptions.HTTPError:
            if attempt < max_retries - 1:
                wait = 2**attempt * 2
                logger.debug(
                    f"Lakehouse properties API failed, "
                    f"retrying in {wait}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait)
                continue
            logger.warning(
                f"Failed to fetch lakehouse properties after {max_retries} attempts, "
                f"defaulting to empty"
            )
            return {}
        except Exception as e:
            logger.warning(f"Failed to fetch lakehouse properties, defaulting to empty: {e}")
            return {}

    logger.warning(
        f"Failed to fetch lakehouse properties after {max_retries} retries (429), "
        f"defaulting to empty"
    )
    return {}


class LivySession:
    def __init__(self, credentials: FabricSparkCredentials):
        self.credential = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = None
        self.is_new_session_required = True
        self.is_local_mode = credentials.is_local_mode

    def __enter__(self) -> LivySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return True

    def try_reuse_session(self, session_id: str) -> bool:
        """Try to reuse an existing session by ID.

        Checks if the session exists in Livy and is in a usable state.

        Parameters
        ----------
        session_id : str
            The session ID to try to reuse.

        Returns
        -------
        bool
            True if session was successfully reused, False otherwise.
        """
        try:
            logger.debug(f"Attempting to reuse existing session: {session_id}")
            self.session_id = session_id

            # Check if session exists and is valid
            res = requests.get(
                self.connect_url + "/sessions/" + session_id,
                headers=get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            )

            # If session doesn't exist (404 or other error), return False
            if res.status_code != 200:
                logger.debug(f"Session {session_id} not found (status: {res.status_code})")
                self.session_id = None
                return False

            res_json = res.json()

            # Check session state
            invalid_states = ["dead", "shutting_down", "killed", "error", "not_found"]

            if self.is_local_mode:
                current_state = res_json.get("state", "dead")
                top_level_state = current_state
            else:
                # Fabric mode: check both top-level state and livyInfo
                # When session is starting, livyInfo may not exist yet
                top_level_state = res_json.get("state", "")
                livy_info = res_json.get("livyInfo", {})
                current_state = livy_info.get("currentState", "")

                # If livyInfo doesn't exist yet but top-level state shows starting, it's still valid
                if not current_state and top_level_state in ("starting", "not_started"):
                    current_state = top_level_state

            if current_state in invalid_states:
                logger.debug(f"Session {session_id} is in invalid state: {current_state}")
                self.session_id = None
                return False

            # Check if session is idle (ready to use) or starting
            if self.is_local_mode:
                if current_state == "idle":
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True
                elif current_state in ("starting", "not_started", "busy"):
                    # Wait for session to become idle
                    logger.debug(f"Session {session_id} is {current_state}, waiting...")
                    self._wait_for_existing_session(session_id)
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True
            else:
                if current_state == "idle":
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True
                elif current_state in ("starting", "not_started", "busy") or top_level_state in (
                    "starting",
                    "not_started",
                ):
                    logger.debug(
                        f"Session {session_id} is {current_state} (top: {top_level_state}), waiting..."
                    )
                    self._wait_for_existing_session(session_id)
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True

            logger.warning(
                f"Session {session_id} in unexpected state: "
                f"top={top_level_state}, livy={current_state}. "
                f"Response: {json.dumps(res_json)[:500]}. Will create new session."
            )
            self.session_id = None
            return False

        except requests.exceptions.RequestException as ex:
            logger.warning(f"Error checking session {session_id}: {ex}. Will create new session.")
            self.session_id = None
            return False
        except Exception as ex:
            logger.warning(f"Unexpected error reusing session {session_id}: {ex}. Will create new session.")
            self.session_id = None
            return False

    def _wait_for_existing_session(self, session_id: str) -> None:
        """Wait for an existing session to become idle."""
        deadline = time.time() + self.credential.session_start_timeout

        while time.time() < deadline:
            res = requests.get(
                self.connect_url + "/sessions/" + session_id,
                headers=get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            ).json()

            if self.is_local_mode:
                state = res.get("state", "")
                if state == "idle":
                    return
                elif state in ("dead", "error", "killed"):
                    raise FailedToConnectError(
                        f"Session {session_id} died while waiting. "
                        f"State: {state}. Response: {json.dumps(res)[:500]}"
                    )
                else:
                    logger.debug(f"Session {session_id} is {state}, waiting...")
            else:
                # Fabric mode: check both top-level state and livyInfo
                top_level_state = res.get("state", "")
                livy_info = res.get("livyInfo", {})
                livy_state = livy_info.get("currentState", "")

                if livy_state == "idle":
                    return
                elif livy_state in ("dead", "error", "killed") or top_level_state in (
                    "dead",
                    "error",
                    "killed",
                ):
                    error_info = res.get("errorInfo", [])
                    err_detail = error_info[0].get("message", "") if error_info else ""
                    raise FailedToConnectError(
                        f"Session {session_id} died while waiting. "
                        f"State: top={top_level_state}, livy={livy_state}. "
                        f"{err_detail}. Response: {json.dumps(res)[:500]}"
                    )
                else:
                    # Session still starting or in transition
                    logger.debug(
                        f"Session {session_id} state: top={top_level_state}, livy={livy_state}, waiting..."
                    )

            time.sleep(self.credential.poll_wait)

        raise FailedToConnectError(
            f"Timeout ({self.credential.session_start_timeout}s) waiting for session {session_id} to become idle"
        )

    def create_session(self, spark_config) -> str:
        # Create sessions
        response = None
        logger.debug("Creating Livy session (this may take a few minutes)")

        # For local Livy, we need to use "kind" parameter instead of "name"
        if self.is_local_mode:
            # Local Livy expects {"kind": "sql"} or {"kind": "spark"}
            session_data = {"kind": "sql"}
            if "kind" in spark_config:
                session_data["kind"] = spark_config["kind"]
        else:
            session_data = spark_config

        try:
            response = requests.post(
                self.connect_url + "/sessions",
                data=json.dumps(session_data),
                headers=get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            )

            # Fabric returns 430 (or 429) when capacity has too many concurrent Spark sessions
            if response.status_code in (429, 430):
                raise FailedToConnectError(
                    f"Fabric Spark rate limit (HTTP {response.status_code}): "
                    f"too many concurrent Spark sessions on this capacity. "
                    f"Cancel active jobs in Fabric Monitoring Hub, "
                    f"use a larger capacity SKU, or try again later. "
                    f"Tip: set reuse_session: true in profiles.yml to share sessions."
                )

            if response.status_code in (200, 201):
                logger.debug("Initiated Livy Session...")
            response.raise_for_status()
        except FailedToConnectError:
            raise
        except requests.exceptions.ConnectionError as c_err:
            err_detail = c_err.response.json() if c_err.response else str(c_err)
            raise FailedToConnectError(f"Connection Error: {err_detail}")
        except requests.exceptions.HTTPError as h_err:
            err_detail = h_err.response.json() if h_err.response else str(h_err)
            raise FailedToConnectError(f"HTTP Error {h_err.response.status_code}: {err_detail}")
        except requests.exceptions.Timeout as t_err:
            err_detail = t_err.response.json() if t_err.response else str(t_err)
            raise FailedToConnectError(f"Timeout Error: {err_detail}")
        except Exception as ex:
            raise FailedToConnectError(str(ex)) from ex

        if response is None:
            raise FailedToConnectError(
                "Session creation returned no response from Fabric Livy API. "
                "Check endpoint URL, workspace ID, and lakehouse ID in profiles.yml."
            )

        self.session_id = None
        try:
            res_body = response.json()
            session_id = res_body.get("id")
            if session_id is None:
                raise FailedToConnectError(
                    f"Session creation response has no 'id' field. "
                    f"Response: {json.dumps(res_body)[:500]}"
                )
            self.session_id = str(session_id)
        except (requests.exceptions.JSONDecodeError, ValueError) as json_err:
            raise FailedToConnectError(
                f"Cannot parse session creation response: {response.text[:500]}"
            ) from json_err

        # Wait for the session to start
        self.wait_for_session_start()

        logger.debug("Livy session created successfully")
        return self.session_id

    def wait_for_session_start(self) -> None:
        """Wait for the Livy session to reach the 'idle' state.

        Fabric's SessionResponse has three state fields (all optional per swagger):
          - state: top-level Livy state (starting/idle/dead/error/...)
          - livyInfo.currentState: Livy-side state (mirrors top-level, may lag)
          - fabricSessionStateInfo.state: Fabric acquisition state
            (queued/acquiringSession/error/cancelled/...)

        During early session acquisition, only fabricSessionStateInfo is populated.
        livyInfo appears once the Spark driver is provisioned.
        On non-2xx responses, body is ErrorResponse (no state fields at all).
        """
        deadline = time.time() + self.credential.session_start_timeout
        while True:
            if time.time() > deadline:
                raise FailedToConnectError(
                    f"Timeout ({self.credential.session_start_timeout}s) waiting for session "
                    f"{self.session_id} to start. Increase `session_start_timeout` in profiles.yml."
                )

            http_res = requests.get(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            )

            # Non-2xx: body is ErrorResponse (errorCode + message), not SessionResponse.
            # Don't try to read state/livyInfo from it.
            if http_res.status_code >= 400:
                try:
                    err_body = http_res.json()
                    err_code = err_body.get("errorCode", "")
                    err_msg = err_body.get("message", "")
                except Exception:
                    err_code = f"HTTP {http_res.status_code}"
                    err_msg = http_res.text[:500]
                raise FailedToConnectError(
                    f"Failed to poll session {self.session_id}: "
                    f"{err_code} — {err_msg}"
                )

            res = http_res.json()

            # Local Livy uses "state" directly
            if self.is_local_mode:
                state = res.get("state", "")
                if state in ("starting", "not_started"):
                    time.sleep(self.credential.poll_wait)
                elif state == "idle":
                    logger.debug(f"New livy session id is: {self.session_id}")
                    self.is_new_session_required = False
                    break
                elif state in ("dead", "error", "killed"):
                    raise FailedToConnectError(
                        f"Session {self.session_id} died during startup. "
                        f"State: {state}. Response: {json.dumps(res)[:500]}"
                    )
                else:
                    logger.debug(f"Session {self.session_id} state: {state}, waiting...")
                    time.sleep(self.credential.poll_wait)
            else:
                # Fabric: three state levels (all optional per swagger spec)
                top_state = res.get("state", "")
                livy_info = res.get("livyInfo", {})
                livy_state = livy_info.get("currentState", "")
                fabric_info = res.get("fabricSessionStateInfo", {})
                fabric_state = fabric_info.get("state", "")

                # Fabric-side acquisition failure (earliest signal)
                if fabric_state in ("error", "cancelled"):
                    raise FailedToConnectError(
                        f"Fabric failed to acquire Spark session {self.session_id}. "
                        f"fabricSessionStateInfo.state={fabric_state}. "
                        f"Response: {json.dumps(res)[:1000]}"
                    )

                # Terminal Livy states
                if top_state in ("dead", "error", "killed", "not_submitted") or \
                   livy_state in ("dead", "error", "killed"):
                    error_info = res.get("errorInfo", [])
                    err_detail = error_info[0].get("message", "") if error_info else ""
                    raise FailedToConnectError(
                        f"Spark session {self.session_id} failed to start. "
                        f"state={top_state}, livy={livy_state}, fabric={fabric_state}. "
                        f"{err_detail}. Response: {json.dumps(res)[:1000]}"
                    )

                # Session is ready
                if livy_state == "idle" or top_state == "idle":
                    logger.debug(f"Livy session {self.session_id} is idle and ready")
                    self.is_new_session_required = False
                    break

                # Still starting — log all three states for debuggability
                logger.debug(
                    f"Session {self.session_id} starting: "
                    f"state={top_state}, livy={livy_state}, fabric={fabric_state}"
                )
                time.sleep(self.credential.poll_wait)

    def delete_session(self) -> None:
        try:
            if self.session_id is None:
                logger.info("Skipping livy session delete: session_id is None")
                return
            # delete the session_id
            res = requests.delete(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            )
            if res.status_code == 200:
                logger.debug(f"Closed the livy session: {self.session_id}")
            else:
                res.raise_for_status()

        except Exception as ex:
            logger.error(f"Unable to close the livy session {self.session_id}, error: {ex}")

    def is_valid_session(self) -> bool:
        if self.session_id is None:
            logger.error("Session ID is None")
            return False
        try:
            res = requests.get(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            ).json()
        except Exception as ex:
            logger.debug(f"is_valid_session HTTP error: {ex}")
            return False

            # we can reuse the session so long as it is not dead, killed, or being shut down
        invalid_states = ["dead", "shutting_down", "killed", "error"]

        # Local Livy uses "state" directly, Fabric uses "livyInfo.currentState"
        if self.is_local_mode:
            current_state = res.get("state", "dead")
        else:
            # Fabric mode: check both top-level state and livyInfo
            # When session is starting, livyInfo may not exist yet
            top_level_state = res.get("state", "")
            livy_info = res.get("livyInfo", {})
            current_state = livy_info.get("currentState", "")

            # If livyInfo doesn't exist yet but top-level state is valid, use that
            if not current_state:
                current_state = top_level_state if top_level_state else "dead"

        return current_state not in invalid_states


# cursor object - wrapped for livy API
class LivyCursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self, credential, livy_session) -> None:
        self._rows = None
        self._schema = None
        self._fetch_index = 0
        self.credential = credential
        self.connect_url = credential.lakehouse_endpoint
        self.session_id = livy_session.session_id
        self.livy_session = livy_session
        self.is_local_mode = credential.is_local_mode

    def __enter__(self) -> LivyCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, bool]]:
        """
        Get the description.

        Returns
        -------
        out : list[tuple[str, str, None, None, None, None, bool]]
            The description.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
        if self._schema is None:
            description = list()
        else:
            description = [
                (
                    field["name"],
                    field["type"],  # field['dataType'],
                    None,
                    None,
                    None,
                    None,
                    field["nullable"],
                )
                for field in self._schema
            ]
        return description

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        self._rows = None

    def _submitLivyCode(self, code) -> Response:
        if self.livy_session.is_new_session_required:
            LivySessionManager.connect(self.credential)
            self.session_id = self.livy_session.session_id

        # Submit code with retry for transient 5xx and 429 (rate-limit) errors
        data = {"code": code, "kind": "sql"}
        url = self.connect_url + "/sessions/" + self.session_id + "/statements"
        logger.debug(f"Submitted: {data} {url}")

        max_retries = 5
        res = None
        for attempt in range(max_retries):
            try:
                res = requests.post(
                    url,
                    data=json.dumps(data),
                    headers=get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                )
            except (
                requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                if attempt >= max_retries - 1:
                    raise DbtRuntimeError(
                        f"Livy statement submit failed after {max_retries} retries: {exc}"
                    )
                wait_time = 2**attempt * 1
                logger.debug(
                    f"Livy statement submit got transient network error "
                    f"({type(exc).__name__}: {exc}), retrying in {wait_time}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                continue
            if res.status_code == 429:
                retry_after = _parse_retry_after(res)
                wait_time = max(retry_after, 2**attempt * 1)
                logger.debug(
                    f"Livy statement submit got HTTP 429, "
                    f"retrying in {wait_time:.0f}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                continue
            if res.status_code < 500:
                break
            if attempt < max_retries - 1:
                wait_time = 2**attempt * 1  # 1s, 2s, 4s, 8s
                logger.debug(
                    f"Livy statement submit got HTTP {res.status_code}, "
                    f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)

        if res.status_code >= 400:
            raise DbtRuntimeError(
                f"Livy statement submit failed (HTTP {res.status_code}): {res.text}"
            )
        json_body = res.json()
        if "id" not in json_body:
            raise DbtRuntimeError(
                f"Livy statement submit returned unexpected response (missing 'id'): {json_body}"
            )
        return res

    def _getLivySQL(self, sql) -> str:
        # Comment, what is going on?!
        # The following code is actually injecting SQL to pyspark object for executing it via the Livy session - over an HTTP post request.
        # Basically, it is like code inside a code. As a result the strings passed here in 'escapedSQL' variable are unescapted and interpreted on the server side.
        # This may have repurcursions of code injection not only as SQL, but also arbritary Python code. An alternate way safer way to acheive this is still unknown.
        # TODO: since the above code is not changed to sending direct SQL to the livy backend, client side string escaping is probably not needed

        code = re.sub(r"\s*/\*(.|\n)*?\*/\s*", "\n", sql, re.DOTALL).strip()
        return code

    def _getLivyResult(self, res_obj) -> Response:
        json_res = res_obj.json()
        stmt_id = json_res.get("id")
        if stmt_id is None:
            raise DbtDatabaseError(
                f"Statement submit response has no 'id'. Response: {json.dumps(json_res)[:500]}"
            )
        statement_id = repr(stmt_id)
        url = self.connect_url + "/sessions/" + self.session_id + "/statements/" + statement_id
        # statement_timeout == 0 means no timeout (poll indefinitely), matching
        # the pre-1.9.5 behavior where long-running models were never interrupted.
        deadline = (
            (time.time() + self.credential.statement_timeout)
            if self.credential.statement_timeout > 0
            else None
        )
        consecutive_failures = 0
        max_poll_retries = 30
        # Adaptive polling: start small so quick statements don't sit idle, grow
        # to a cap so slow statements don't hammer the server. Initial value is
        # intentionally not *too* small — Fabric Livy sometimes returns 404 for a
        # just-submitted statement id that has not yet registered on the server
        # (handled by the 404 retry block below).
        _poll_interval = 0.3
        _poll_interval_cap = max(self.credential.poll_statement_wait * 3, 1.5)
        # 404 can appear transiently right after submit before the statement id
        # is registered, or when the Fabric Livy service briefly loses track of
        # the session/statement. Retry with exponential backoff before giving up.
        not_found_retries = 0
        max_not_found_retries = 20
        while True:
            if deadline is not None and time.time() > deadline:
                raise DbtDatabaseError(
                    f"Timeout ({self.credential.statement_timeout}s) waiting for statement "
                    f"{statement_id} to complete. Increase `statement_timeout` in profiles.yml."
                )
            try:
                poll_res = requests.get(
                    url,
                    headers=get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                )
            except (
                requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                consecutive_failures += 1
                if consecutive_failures > max_poll_retries:
                    raise DbtRuntimeError(
                        f"Livy statement poll failed after {max_poll_retries} retries "
                        f"({type(exc).__name__}: {exc})"
                    )
                wait_time = min(2 ** (consecutive_failures - 1), 30)
                logger.debug(
                    f"Livy statement poll got transient network error "
                    f"({type(exc).__name__}: {exc}), retrying in {wait_time}s "
                    f"(attempt {consecutive_failures}/{max_poll_retries})"
                )
                time.sleep(wait_time)
                continue
            if poll_res.status_code == 429:
                consecutive_failures += 1
                retry_after = _parse_retry_after(poll_res)
                wait_time = max(retry_after, 2 ** (consecutive_failures - 1) * 1)
                logger.debug(
                    f"Livy statement poll got HTTP 429, "
                    f"retrying in {wait_time:.0f}s (attempt {consecutive_failures}/{max_poll_retries})"
                )
                time.sleep(wait_time)
                if consecutive_failures > max_poll_retries:
                    raise DbtRuntimeError(
                        f"Livy statement poll failed after {max_poll_retries} retries "
                        f"(HTTP 429): {poll_res.text}"
                    )
                continue
            if poll_res.status_code >= 500:
                consecutive_failures += 1
                if consecutive_failures <= max_poll_retries:
                    wait_time = 2 ** (consecutive_failures - 1) * 1  # 1s, 2s, 4s, ...
                    logger.debug(
                        f"Livy statement poll got HTTP {poll_res.status_code}, "
                        f"retrying in {wait_time}s (attempt {consecutive_failures}/{max_poll_retries})"
                    )
                    time.sleep(wait_time)
                    continue
                raise DbtRuntimeError(
                    f"Livy statement poll failed after {max_poll_retries} retries "
                    f"(HTTP {poll_res.status_code}): {poll_res.text}"
                )
            if poll_res.status_code == 404 and not_found_retries < max_not_found_retries:
                # Statement id not yet visible on the server; back off briefly and retry.
                not_found_retries += 1
                wait_time = min(0.3 * (2.0 ** (not_found_retries - 1)), 5.0)
                logger.debug(
                    f"Livy statement poll got HTTP 404, retrying in {wait_time:.2f}s "
                    f"(not-found attempt {not_found_retries}/{max_not_found_retries})"
                )
                time.sleep(wait_time)
                continue
            if poll_res.status_code >= 400:
                raise DbtRuntimeError(
                    f"Livy statement poll failed (HTTP {poll_res.status_code}): {poll_res.text}"
                )
            consecutive_failures = 0
            res = poll_res.json()
            if "state" not in res:
                raise DbtRuntimeError(
                    f"Livy statement poll returned unexpected response (missing 'state'): {res}"
                )

            if res["state"] == "available":
                return res
            elif res["state"] in ("error", "cancelled", "cancelling"):
                error_msg = res.get("output", {}).get("evalue", "Unknown error")
                raise DbtDatabaseError(
                    f"Statement {statement_id} failed with state '{res['state']}': {error_msg}"
                )
            time.sleep(_poll_interval)
            _poll_interval = min(_poll_interval * 1.5, _poll_interval_cap)

    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Raises
        ------
        NotImplementedError
            If there are parameters given. We do not format sql statements.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        if len(parameters) > 0:
            sql = sql % parameters

        # TODO: handle parameterised sql

        retries = self.credential.connect_retries
        retry_wait = self.credential.connect_timeout
        last_error = None

        for attempt in range(1 + retries):
            res = self._getLivyResult(self._submitLivyCode(self._getLivySQL(sql)))
            logger.debug(res)

            output = res.get("output")
            if output is None:
                raise DbtDatabaseError(
                    f"Statement response has no 'output' field. "
                    f"Full response: {json.dumps(res)[:1000]}"
                )
            output_status = output.get("status", "")

            if output_status == "ok":
                output_data = output.get("data", {})
                # Local and Fabric Livy have different output structures
                if self.is_local_mode:
                    if "application/json" in output_data:
                        values = output_data["application/json"]
                        if isinstance(values, dict) and "data" in values:
                            self._rows = values["data"]
                            self._schema = values.get("schema", {}).get("fields", [])
                        elif isinstance(values, list):
                            self._rows = values
                            self._schema = []
                        else:
                            self._rows = []
                            self._schema = []
                    elif "text/plain" in output_data:
                        self._rows = []
                        self._schema = []
                    else:
                        self._rows = []
                        self._schema = []
                else:
                    # Fabric Livy format
                    values = output_data.get("application/json", {})
                    if values and isinstance(values, dict):
                        self._rows = values.get("data", [])
                        self._schema = values.get("schema", {}).get("fields", [])
                    else:
                        self._rows = []
                        self._schema = []
                return

            last_error = output.get("evalue") or output.get("traceback") or (
                f"Unknown error (status={output_status}). "
                f"Full output: {json.dumps(output)[:500]}. "
                f"Full response: {json.dumps(res)[:500]}"
            )
            # Retry on timeout errors (e.g. Spark cold-start TimeoutException)
            if "timeout" in last_error.lower() and attempt < retries:
                logger.warning(
                    f"Query timed out (attempt {attempt + 1}/{1 + retries}), "
                    f"retrying in {retry_wait}s..."
                )
                time.sleep(retry_wait)
                continue

            # Non-timeout error or exhausted retries — fail immediately
            break

        self._rows = None
        self._schema = None
        raise DbtDatabaseError("Error while executing query: " + last_error)

    def fetchall(self):
        """
        Fetch all data.

        Returns
        -------
        out : list() | None
            The rows.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchall
        """
        return self._rows

    def fetchone(self):
        """
        Fetch the first output.

        Returns
        -------
        out : one row | None
            The first row.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """

        if self._rows is not None and self._fetch_index < len(self._rows):
            row = self._rows[self._fetch_index]
            self._fetch_index += 1
        else:
            row = None

        return row


class LivyConnection:
    """
    Mock a pyodbc connection.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Connection
    """

    def __init__(self, credentials, livy_session) -> None:
        self.credential: FabricSparkCredentials = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = livy_session.session_id

        self._cursor = LivyCursor(self.credential, livy_session)

    def get_session_id(self) -> str:
        return self.session_id

    def get_headers(self) -> dict[str, str]:
        return get_headers(self.credential, False)

    def get_connect_url(self) -> str:
        return self.connect_url

    def cursor(self) -> LivyCursor:
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return self._cursor

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        logger.debug("Connection.close()")
        self._cursor.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True


def _atexit_cleanup() -> None:
    """Delete the Fabric Livy session on process exit.

    Local-mode sessions are kept alive for reuse across runs.
    """
    LivySessionManager.disconnect()


atexit.register(_atexit_cleanup)


# TODO: How to authenticate
class LivySessionManager:
    livy_global_session = None
    _session_create_cond = threading.Condition()
    _session_create_in_progress = False

    @staticmethod
    def connect(credentials: FabricSparkCredentials) -> LivyConnection:
        """Connect to a Livy session.

        For local mode: reuses existing sessions via session ID file persistence.
        For Fabric mode: creates a new session or reuses an existing one.

        This method is both thread-safe (via ``_session_lock``) and
        process-safe (via ``cross_process_session_lock``).  When multiple
        dbt processes start simultaneously (e.g. parallel Claude Code
        sub-agent commands), the cross-process file lock ensures only one
        process creates the Spark session while others wait and reuse it.
        """
        session_file = credentials.resolved_session_id_file

        # Cross-process lock: prevents 7 parallel dbt processes from each
        # creating their own Spark session.  The first process to acquire
        # the lock creates the session and writes its ID to the session
        # file.  Waiting processes re-check the file after the lock is
        # released and reuse the existing session.
        with cross_process_session_lock(session_file):
            with _session_lock:
                spark_config = credentials.spark_config

                if credentials.is_local_mode:
                    LivySessionManager._connect_local(credentials, spark_config)
                else:
                    LivySessionManager._connect_fabric(credentials, spark_config)

                livyConnection = LivyConnection(credentials, LivySessionManager.livy_global_session)
                return livyConnection

    @staticmethod
    def _connect_local(credentials: FabricSparkCredentials, spark_config) -> None:
        """Connect in local mode with session file reuse.

        Local mode persists the Livy session ID to a file so that subsequent
        dbt invocations can reuse the same session instead of creating a new one.

        Connection strategy (in order):
        1. Reuse the in-memory session if it's still valid and ready.
        2. Read the session ID from the persisted file and try to reattach.
           Skip if the file contains the same ID we already hold (it already
           failed validity above, so retrying would be redundant).
        3. Create a brand-new session and persist its ID to the file.
        """
        session_file_path = credentials.resolved_session_id_file
        session = LivySessionManager.livy_global_session

        # 1. Fast path: reuse current in-memory session if it's valid and idle
        if (
            session is not None
            and session.is_valid_session()
            and not session.is_new_session_required
        ):
            logger.debug(f"Reusing session: {session.session_id}")
            return

        # Ensure we have a LivySession instance to work with
        if session is None:
            session = LivySession(credentials)
            LivySessionManager.livy_global_session = session

        # 2. Try to reattach to an existing session persisted in the file.
        #    Skip if the file holds the same session ID we already have —
        #    that session was just found invalid above, no point retrying.
        existing_session_id = read_session_id_from_file(session_file_path)
        if existing_session_id and existing_session_id != session.session_id:
            if session.try_reuse_session(existing_session_id):
                logger.debug(f"Reused session from file: {existing_session_id}")
                return

        # 3. No reusable session available — create a new one and persist its ID
        LivySessionManager._create_and_persist_session(spark_config, session_file_path)

    @staticmethod
    def _connect_fabric(credentials: FabricSparkCredentials, spark_config) -> None:
        """Connect in Fabric mode.

        When reuse_session is False (default):
          Creates a new session each time unless there is already a valid,
          ready session in memory. Session is deleted at exit.

        When reuse_session is True:
          Reuses existing sessions via session ID file persistence, similar
          to local mode. Session is kept alive at exit for reuse by subsequent
          dbt runs. Fabric will auto-kill it after the configured idle timeout.

        After session creation, any configured OneLake shortcuts are also created.
        """
        if credentials.reuse_session:
            LivySessionManager._connect_fabric_reuse(credentials, spark_config)
        else:
            LivySessionManager._connect_fabric_fresh(credentials, spark_config)

    @staticmethod
    def _connect_fabric_fresh(credentials: FabricSparkCredentials, spark_config) -> None:
        """Connect in Fabric mode — creates a new session or reuses one.

        Even when ``reuse_session`` is False, this method now checks the
        session ID file for cross-process sharing.  When multiple dbt
        processes start simultaneously (e.g. parallel sub-agent commands),
        the first process creates the session and writes its ID to the
        file.  Later processes (arriving after the cross-process lock is
        released) read the file and reuse that session instead of creating
        a duplicate.

        The session is still deleted at process exit (atexit handler),
        unless ``reuse_session`` is True.
        """
        session = LivySessionManager.livy_global_session

        # 1. Fast path: reuse current in-memory session if it's valid
        if (
            session is not None
            and session.is_valid_session()
            and not session.is_new_session_required
        ):
            logger.debug(f"Reusing in-memory session: {session.session_id}")
            return

        # 2. Cross-process path: another process may have created a session
        #    while we were waiting on the file lock.  Check the session file.
        session_file_path = credentials.resolved_session_id_file
        existing_session_id = read_session_id_from_file(session_file_path)
        if existing_session_id:
            if session is None:
                session = LivySession(credentials)
                LivySessionManager.livy_global_session = session
            if session.session_id != existing_session_id:
                if session.try_reuse_session(existing_session_id):
                    logger.info(
                        f"Reusing session from another process: {existing_session_id}"
                    )
                    return

        # 3. No reusable session — create a new one and persist for siblings
        LivySessionManager._create_fabric_session(credentials, spark_config)
        write_session_id_to_file(
            session_file_path,
            LivySessionManager.livy_global_session.session_id,
        )

    @staticmethod
    def _connect_fabric_reuse(credentials: FabricSparkCredentials, spark_config) -> None:
        """Connect in Fabric mode with session reuse across runs.

        Connection strategy (same as local mode):
        1. Reuse the in-memory session if it's still valid and ready.
        2. Read the session ID from the persisted file and try to reattach.
        3. Create a brand-new session and persist its ID to the file.
        """
        session_file_path = credentials.resolved_session_id_file
        session = LivySessionManager.livy_global_session

        # 1. Fast path: reuse current in-memory session if it's valid and idle
        if (
            session is not None
            and session.is_valid_session()
            and not session.is_new_session_required
        ):
            logger.debug(f"Reusing Fabric session: {session.session_id}")
            return

        # Ensure we have a LivySession instance to work with
        if session is None:
            session = LivySession(credentials)
            LivySessionManager.livy_global_session = session

        # 2. Try to reattach to an existing session persisted in the file.
        existing_session_id = read_session_id_from_file(session_file_path)
        if existing_session_id and existing_session_id != session.session_id:
            if session.try_reuse_session(existing_session_id):
                logger.info(f"Reused existing Fabric session from file: {existing_session_id}")
                return

        # 3. No reusable session — create a new one and persist its ID
        LivySessionManager._create_fabric_session(credentials, spark_config)
        write_session_id_to_file(
            session_file_path,
            LivySessionManager.livy_global_session.session_id,
        )

    @staticmethod
    def _create_fabric_session(credentials: FabricSparkCredentials, spark_config) -> None:
        """Create a new Fabric Livy session and set up shortcuts."""
        LivySessionManager.livy_global_session = LivySession(credentials)

        # Inject environmentId into spark_config if configured
        if credentials.environmentId:
            spark_config = {
                **spark_config,
                "conf": {
                    **spark_config.get("conf", {}),
                    "spark.fabric.environment.id": credentials.environmentId,
                },
            }
            logger.debug(f"Using Fabric Environment: {credentials.environmentId}")

        # Inject session idle timeout into spark_config
        if credentials.session_idle_timeout:
            spark_config = {
                **spark_config,
                "conf": {
                    **spark_config.get("conf", {}),
                    "spark.livy.session.idle.timeout": credentials.session_idle_timeout,
                },
            }
            logger.debug(f"Session idle timeout: {credentials.session_idle_timeout}")

        LivySessionManager.livy_global_session.create_session(spark_config)
        LivySessionManager.livy_global_session.is_new_session_required = False

        # Create OneLake shortcuts if configured
        if credentials.create_shortcuts:
            try:
                shortcut_client = ShortcutClient(
                    accessToken.token,
                    credentials.workspaceid,
                    credentials.lakehouseid,
                    credentials.endpoint,
                )
                shortcut_client.create_shortcuts(credentials.shortcuts_json_str)
            except Exception as ex:
                logger.error(f"Unable to create shortcuts: {ex}")

    @staticmethod
    def _create_and_persist_session(spark_config, session_file_path: str) -> None:
        """Create a new session and write the session ID to file (local mode only)."""
        LivySessionManager.livy_global_session.create_session(spark_config)
        LivySessionManager.livy_global_session.is_new_session_required = False
        write_session_id_to_file(
            session_file_path, LivySessionManager.livy_global_session.session_id
        )

    @staticmethod
    def disconnect() -> None:
        """Disconnect from the session manager.

        - Local mode: keeps the Livy session alive for reuse.
        - Fabric mode with reuse_session=True: keeps session alive for reuse.
        - Fabric mode with reuse_session=False: deletes the session.

        This method is thread-safe.
        """
        with _session_lock:
            if LivySessionManager.livy_global_session is None:
                logger.debug("No session to disconnect")
                return

            session = LivySessionManager.livy_global_session
            session_id = session.session_id

            if session.is_local_mode or session.credential.reuse_session:
                # Local mode or Fabric reuse mode: keep the session alive
                logger.debug(
                    f"Disconnecting from session manager (session {session_id} kept alive for reuse)"
                )
            else:
                # Fabric mode: delete the session since it won't be reused
                logger.debug(f"Deleting Fabric Livy session: {session_id}")
                session.delete_session()

            # Reset the local reference in both cases
            LivySessionManager.livy_global_session = None


class LivySessionConnectionWrapper(object):
    """Connection wrapper for the livy sessoin connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self) -> LivySessionConnectionWrapper:
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        logger.debug("NotImplemented: cancel")

    def close(self):
        self.handle.close()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchmany(self, size=None):
        rows = self._cursor.fetchall()
        if rows is None:
            return []
        if size is not None:
            return rows[:size]
        return rows

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is None:
            self._cursor.execute(sql)
        else:
            bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value) -> float | str:
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif value is None:
            return "''"
        else:
            escaped = str(value).replace("'", "\\'")
            return f"'{escaped}'"
