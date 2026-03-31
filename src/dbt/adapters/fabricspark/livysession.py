from __future__ import annotations

import datetime as dt
import json
import os
import re
import threading
import time
from types import TracebackType
from typing import Any
from urllib import response

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
    global accessToken
    # env_oauth_access_token: always read fresh from env var, never cache
    if credentials.authentication and credentials.authentication.lower() == "env_oauth_access_token":
        if accessToken is None:
            logger.info("Using env_oauth_access_token auth (reading FABRIC_LAKEHOUSE_ACCESS_TOKEN)")
        accessToken = get_env_access_token()
    elif accessToken is None or is_token_refresh_necessary(accessToken.expires_on):
        if credentials.authentication and credentials.authentication.lower() == "cli":
            logger.info("Using CLI auth")
            accessToken = get_cli_access_token(credentials)
        elif credentials.authentication and credentials.authentication.lower() == "int_tests":
            logger.info("Using int_tests auth")
            accessToken = get_default_access_token(credentials)
        elif (
            credentials.authentication and credentials.authentication.lower() == "fabric_notebook"
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


def _get_session_cache_path(credentials: FabricSparkCredentials) -> str:
    # Support both session_id_file (from reference) and livy_session_path (current)
    if credentials.session_id_file:
        return credentials.session_id_file
    if credentials.livy_session_path:
        return credentials.livy_session_path
    target_path = os.getenv("DBT_TARGET_PATH") or os.path.join(os.getcwd(), "target")
    return os.path.join(target_path, "fabricspark_livy_session_id")


def _read_cached_session_id(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            value = handle.read().strip()
        return value if value else None
    except Exception as ex:
        logger.debug(f"Unable to read cached livy session id from {path}: {ex}")
        return None


def _write_cached_session_id(path: str, session_id: str) -> None:
    try:
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(session_id)
    except Exception as ex:
        logger.debug(f"Unable to write cached livy session id to {path}: {ex}")


def _clear_cached_session_id(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as ex:
        logger.debug(f"Unable to clear cached livy session id at {path}: {ex}")


class LivySession:
    def __init__(self, credentials: FabricSparkCredentials):
        self.credential = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = None
        self.is_new_session_required = True

    def __enter__(self) -> LivySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return True

    def create_session(self, data) -> str:
        # Create sessions
        response = None
        logger.debug("Creating Livy session (this may take a few minutes)")
        try:
            response = requests.post(
                self.connect_url + "/sessions",
                data=json.dumps(data),
                headers=get_headers(self.credential, False),
            )
            if response.status_code == 200:
                logger.debug("Initiated Livy Session...")
            response.raise_for_status()
        except requests.exceptions.ConnectionError as c_err:
            raise Exception("Connection Error :", c_err.response.json())
        except requests.exceptions.HTTPError as h_err:
            raise Exception("Http Error: ", h_err.response.json())
        except requests.exceptions.Timeout as t_err:
            raise Exception("Timeout Error: ", t_err.response.json())
        except requests.exceptions.RequestException as a_err:
            raise Exception("Authorization Error: ", a_err.response.json())
        except Exception as ex:
            raise Exception(ex) from ex

        if response is None:
            raise Exception("Invalid response from Livy server")

        self.session_id = None
        try:
            self.session_id = str(response.json()["id"])
        except requests.exceptions.JSONDecodeError as json_err:
            raise Exception("Json decode error to get session_id") from json_err

        # Wait for the session to start
        self.wait_for_session_start()

        logger.debug("Livy session created successfully")
        return self.session_id

    def wait_for_session_start(self) -> None:
        """Wait for the Livy session to reach the 'idle' state."""
        while True:
            res = requests.get(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
            ).json()
            if res["state"] == "starting" or res["state"] == "not_started":
                time.sleep(DEFAULT_POLL_WAIT)
            elif res["livyInfo"]["currentState"] == "idle":
                logger.debug(f"New livy session id is: {self.session_id}, {res}")
                self.is_new_session_required = False
                break
            elif res["livyInfo"]["currentState"] == "dead":
                logger.error("ERROR, cannot create a livy session")
                raise FailedToConnectError("failed to connect")

    def delete_session(self) -> None:
        try:
            if self.session_id is None:
                logger.info("Skipping livy session delete: session_id is None")
                return
            # delete the session_id
            _ = requests.delete(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
            )
            if _.status_code == 200:
                logger.debug(f"Closed the livy session: {self.session_id}")
            else:
                response.raise_for_status()

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
            ).json()
            # we can reuse the session so long as it is not dead, killed, or being shut down
            invalid_states = ["dead", "shutting_down", "killed"]
            return res["livyInfo"]["currentState"] not in invalid_states
        except Exception as ex:
            logger.debug(f"Failed to validate livy session {self.session_id}: {ex}")
            return False


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
        self.credential = credential
        self.connect_url = credential.lakehouse_endpoint
        self.session_id = livy_session.session_id
        self.livy_session = livy_session

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

        # Submit code
        data = {"code": code, "kind": "sql"}
        logger.debug(
            f"Submitted: {data} {self.connect_url + '/sessions/' + self.session_id + '/statements'}"
        )
        res = requests.post(
            self.connect_url + "/sessions/" + self.session_id + "/statements",
            data=json.dumps(data),
            headers=get_headers(self.credential, False),
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
        while True:
            res = requests.get(
                self.connect_url
                + "/sessions/"
                + self.session_id
                + "/statements/"
                + repr(json_res["id"]),
                headers=get_headers(self.credential, False),
            ).json()

            if res["state"] == "available":
                return res
            time.sleep(DEFAULT_POLL_STATEMENT_WAIT)

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
            if res["output"]["status"] == "ok":
                values = res["output"]["data"]["application/json"]
                if len(values) >= 1:
                    self._rows = values["data"]  # values[0]['values']
                    self._schema = values["schema"]["fields"]  # values[0]['schema']
                else:
                    self._rows = []
                    self._schema = []
                return

            last_error = res["output"]["evalue"]
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

        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
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


# TODO: How to authenticate
class LivySessionManager:
    livy_global_session = None
    _session_create_cond = threading.Condition()
    _session_create_in_progress = False

    @staticmethod
    def connect(credentials: FabricSparkCredentials) -> LivyConnection:
        # the following opens an spark / sql session
        data = credentials.spark_config
        reuse_enabled = bool(credentials.reuse_livy_session)
        cache_path = _get_session_cache_path(credentials) if reuse_enabled else None
        logger.info(
            "Livy reuse is %s. Session cache path: %s",
            "enabled" if reuse_enabled else "disabled",
            cache_path if cache_path else "n/a",
        )

        old_session_to_delete = None
        with LivySessionManager._session_create_cond:
            while LivySessionManager._session_create_in_progress:
                logger.info("Waiting for livy session creation to finish")
                LivySessionManager._session_create_cond.wait(timeout=DEFAULT_POLL_WAIT)

            if LivySessionManager.livy_global_session is not None:
                current_session_id = LivySessionManager.livy_global_session.session_id
                if (
                    current_session_id
                    and LivySessionManager.livy_global_session.is_valid_session()
                ):
                    if not LivySessionManager.livy_global_session.is_new_session_required:
                        logger.debug(f"Reusing session: {current_session_id}")
                        return LivyConnection(credentials, LivySessionManager.livy_global_session)
                elif current_session_id:
                    old_session_to_delete = LivySessionManager.livy_global_session

            LivySessionManager._session_create_in_progress = True

        if old_session_to_delete is not None:
            logger.info(
                "Deleting invalid livy session before creating new one: %s",
                old_session_to_delete.session_id,
            )
            old_session_to_delete.delete_session()

        logger.info("Creating new livy session")
        new_session = LivySession(credentials)
        try:
            if reuse_enabled:
                cached_session_id = _read_cached_session_id(cache_path)
                if cached_session_id:
                    new_session.session_id = cached_session_id
                    if new_session.is_valid_session():
                        logger.debug(f"Reusing cached session: {cached_session_id}")
                        new_session.is_new_session_required = False
                    else:
                        new_session.session_id = None
                        new_session.create_session(data)
                        new_session.is_new_session_required = False
                        _write_cached_session_id(cache_path, new_session.session_id)
                else:
                    new_session.create_session(data)
                    new_session.is_new_session_required = False
                    _write_cached_session_id(cache_path, new_session.session_id)
            else:
                new_session.create_session(data)
                new_session.is_new_session_required = False

            # create shortcuts, if there are any
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
        except Exception:
            with LivySessionManager._session_create_cond:
                LivySessionManager._session_create_in_progress = False
                LivySessionManager._session_create_cond.notify_all()
            raise

        with LivySessionManager._session_create_cond:
            LivySessionManager.livy_global_session = new_session
            LivySessionManager._session_create_in_progress = False
            LivySessionManager._session_create_cond.notify_all()

        livyConnection = LivyConnection(credentials, LivySessionManager.livy_global_session)
        return livyConnection

    @staticmethod
    def disconnect() -> None:
        if LivySessionManager.livy_global_session is None:
            logger.debug("No session to disconnect")
            return

        credentials = LivySessionManager.livy_global_session.credential
        reuse_enabled = bool(credentials.reuse_livy_session)
        cache_path = _get_session_cache_path(credentials) if reuse_enabled else None
        logger.info(
            "Disconnect called. Livy reuse is %s. Session cache path: %s. Session id: %s",
            "enabled" if reuse_enabled else "disabled",
            cache_path if cache_path else "n/a",
            LivySessionManager.livy_global_session.session_id,
        )

        if reuse_enabled:
            if not LivySessionManager.livy_global_session.is_valid_session():
                _clear_cached_session_id(cache_path)
            logger.debug("Keeping livy session alive due to reuse_livy_session")
            return

        if LivySessionManager.livy_global_session.is_valid_session():
            LivySessionManager.livy_global_session.delete_session()
            LivySessionManager.livy_global_session.is_new_session_required = True
            if cache_path:
                _clear_cached_session_id(cache_path)
        else:
            logger.debug("No session to disconnect")


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
            return f"'{value}'"
