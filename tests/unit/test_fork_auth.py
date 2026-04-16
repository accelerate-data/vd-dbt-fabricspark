"""Tests for fork-unique authentication methods in livysession.

Covers get_env_access_token, get_vdstudio_oauth_access_token, and
get_headers dispatch for these auth modes.
"""
import os
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from azure.core.credentials import AccessToken
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabricspark.livysession import (
    get_env_access_token,
    get_headers,
    get_vdstudio_oauth_access_token,
)


# ---------------------------------------------------------------------------
# get_env_access_token
# ---------------------------------------------------------------------------


class TestGetEnvAccessToken:
    """Tests for the get_env_access_token function."""

    @patch.dict(os.environ, {"FABRIC_LAKEHOUSE_ACCESS_TOKEN": "mytoken"})
    def test_env_access_token_reads_from_env(self):
        """When FABRIC_LAKEHOUSE_ACCESS_TOKEN is set, returns an AccessToken with that value."""
        result = get_env_access_token()

        assert isinstance(result, AccessToken)
        assert result.token == "mytoken"

    @patch.dict(os.environ, {}, clear=True)
    def test_env_access_token_missing_env_var_raises(self):
        """When the env var is missing, raises DbtRuntimeError."""
        # Ensure the variable is truly absent
        os.environ.pop("FABRIC_LAKEHOUSE_ACCESS_TOKEN", None)

        with pytest.raises(DbtRuntimeError, match="FABRIC_LAKEHOUSE_ACCESS_TOKEN"):
            get_env_access_token()

    @patch.dict(os.environ, {"FABRIC_LAKEHOUSE_ACCESS_TOKEN": "anytoken"})
    def test_env_access_token_expires_on_zero(self):
        """expires_on is always 0 so the token is re-read fresh every time."""
        result = get_env_access_token()

        assert result.expires_on == 0


# ---------------------------------------------------------------------------
# get_vdstudio_oauth_access_token
# ---------------------------------------------------------------------------


class TestGetVdstudioOauthAccessToken:
    """Tests for the get_vdstudio_oauth_access_token function."""

    @patch.dict(
        os.environ,
        {
            "VD_STUDIO_TOKEN_URL": "http://env-url:8080/az_token",
            "VD_STUDIO_USER_ID": "user1",
        },
    )
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_vdstudio_oauth_from_env_url(self, mock_get):
        """When VD_STUDIO_TOKEN_URL is set, it is used as the endpoint URL."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok", "expires_in": 3600}
        mock_get.return_value = mock_resp

        creds = MagicMock()
        creds.vdstudio_oauth_endpoint_url = "http://creds-url:9090/az_token"

        get_vdstudio_oauth_access_token(creds)

        # The env var URL takes precedence over the credentials URL
        call_args = mock_get.call_args
        assert call_args[0][0] == "http://env-url:8080/az_token"

    @patch.dict(os.environ, {"VD_STUDIO_USER_ID": "user1"})
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_vdstudio_oauth_from_credentials(self, mock_get):
        """When env var is absent, uses vdstudio_oauth_endpoint_url from credentials."""
        # Ensure env URL is absent
        os.environ.pop("VD_STUDIO_TOKEN_URL", None)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok", "expires_in": 3600}
        mock_get.return_value = mock_resp

        creds = MagicMock()
        creds.vdstudio_oauth_endpoint_url = "http://localhost:8080/az_token"

        get_vdstudio_oauth_access_token(creds)

        call_args = mock_get.call_args
        assert call_args[0][0] == "http://localhost:8080/az_token"

    @patch.dict(os.environ, {"VD_STUDIO_USER_ID": "user1"})
    def test_vdstudio_oauth_no_url_raises(self):
        """When neither env var nor credentials URL is set, raises DbtRuntimeError."""
        os.environ.pop("VD_STUDIO_TOKEN_URL", None)

        creds = MagicMock()
        creds.vdstudio_oauth_endpoint_url = None

        with pytest.raises(DbtRuntimeError, match="VD_STUDIO_TOKEN_URL"):
            get_vdstudio_oauth_access_token(creds)

    @patch.dict(
        os.environ,
        {"VD_STUDIO_TOKEN_URL": "http://localhost:8080/az_token"},
    )
    def test_vdstudio_oauth_no_user_id_raises(self):
        """When VD_STUDIO_USER_ID is not set, raises DbtRuntimeError."""
        os.environ.pop("VD_STUDIO_USER_ID", None)

        creds = MagicMock()
        creds.vdstudio_oauth_endpoint_url = "http://localhost:8080/az_token"

        with pytest.raises(DbtRuntimeError, match="VD_STUDIO_USER_ID"):
            get_vdstudio_oauth_access_token(creds)

    @patch.dict(
        os.environ,
        {
            "VD_STUDIO_TOKEN_URL": "http://localhost:8080/az_token",
            "VD_STUDIO_USER_ID": "user1",
        },
    )
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_vdstudio_oauth_success(self, mock_get):
        """A successful response returns a valid AccessToken."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok", "expires_in": 3600}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        creds = MagicMock()
        creds.vdstudio_oauth_endpoint_url = None

        before = int(time.time())
        result = get_vdstudio_oauth_access_token(creds)
        after = int(time.time())

        assert isinstance(result, AccessToken)
        assert result.token == "tok"
        # expires_on should be roughly now + 3600
        assert before + 3600 <= result.expires_on <= after + 3600

    @patch.dict(
        os.environ,
        {
            "VD_STUDIO_TOKEN_URL": "http://localhost:8080/az_token",
            "VD_STUDIO_USER_ID": "user1",
        },
    )
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_vdstudio_oauth_connection_error(self, mock_get):
        """When requests.get raises ConnectionError, raises DbtRuntimeError mentioning vd-studio."""
        mock_get.side_effect = requests.exceptions.ConnectionError("refused")

        creds = MagicMock()
        creds.vdstudio_oauth_endpoint_url = None

        with pytest.raises(DbtRuntimeError, match="Is vd-studio running"):
            get_vdstudio_oauth_access_token(creds)

    @patch.dict(
        os.environ,
        {
            "VD_STUDIO_TOKEN_URL": "http://localhost:8080/az_token",
            "VD_STUDIO_USER_ID": "user1",
        },
    )
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_vdstudio_oauth_401_unauthorized(self, mock_get):
        """A 401 response raises DbtRuntimeError mentioning re-login."""
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_get.return_value = mock_resp

        creds = MagicMock()
        creds.vdstudio_oauth_endpoint_url = None

        with pytest.raises(DbtRuntimeError, match="re-login"):
            get_vdstudio_oauth_access_token(creds)

    @patch.dict(
        os.environ,
        {
            "VD_STUDIO_TOKEN_URL": "http://localhost:8080/az_token",
            "VD_STUDIO_USER_ID": "user1",
        },
    )
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_vdstudio_oauth_403_forbidden(self, mock_get):
        """A 403 response raises DbtRuntimeError mentioning localhost."""
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_get.return_value = mock_resp

        creds = MagicMock()
        creds.vdstudio_oauth_endpoint_url = None

        with pytest.raises(DbtRuntimeError, match="localhost"):
            get_vdstudio_oauth_access_token(creds)


# ---------------------------------------------------------------------------
# get_headers auth dispatch
# ---------------------------------------------------------------------------


class TestGetHeadersAuthDispatch:
    """Tests for get_headers routing to fork-unique auth methods."""

    def setup_method(self):
        """Reset the global accessToken before each test."""
        import dbt.adapters.fabricspark.livysession as livysession_module

        livysession_module.accessToken = None

    @patch("dbt.adapters.fabricspark.livysession.get_env_access_token")
    def test_get_headers_dispatches_env_oauth(self, mock_get_env):
        """authentication='env_oauth_access_token' dispatches to get_env_access_token."""
        mock_get_env.return_value = AccessToken(token="env-tok", expires_on=0)

        creds = MagicMock()
        creds.is_local_mode = False
        creds.authentication = "env_oauth_access_token"

        headers = get_headers(creds)

        mock_get_env.assert_called_once()
        assert headers["Authorization"] == "Bearer env-tok"

    @patch("dbt.adapters.fabricspark.livysession.get_vdstudio_oauth_access_token")
    def test_get_headers_dispatches_vdstudio_oauth(self, mock_get_vdstudio):
        """authentication='vdstudio_oauth' dispatches to get_vdstudio_oauth_access_token."""
        mock_get_vdstudio.return_value = AccessToken(
            token="vdstudio-tok", expires_on=9999999999
        )

        creds = MagicMock()
        creds.is_local_mode = False
        creds.authentication = "vdstudio_oauth"

        headers = get_headers(creds)

        mock_get_vdstudio.assert_called_once_with(creds)
        assert headers["Authorization"] == "Bearer vdstudio-tok"
