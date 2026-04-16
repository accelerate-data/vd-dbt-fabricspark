"""Unit tests for the notebook module.

Tests cover DbtJobConfig, _derive_job_name, setup_environment, clone_repo,
_resolve_token, run_dbt_job, and ConnectionConfig.
"""

from __future__ import annotations

import subprocess
from unittest import mock

import pytest

from dbt.adapters.fabricspark.notebook import (
    ConnectionConfig,
    DbtJobConfig,
    RepoConfig,
    run_dbt_job,
)
from dbt.adapters.fabricspark.notebook import _derive_job_name
from dbt.adapters.fabricspark.notebook.environment import setup_environment
from dbt.adapters.fabricspark.notebook.repo import clone_repo, _resolve_token
from dbt.adapters.fabricspark.notebook.runner import DbtResult


# ---------------------------------------------------------------------------
# DbtJobConfig tests
# ---------------------------------------------------------------------------


class TestDbtJobConfig:
    def test_single_command_string_returns_list(self):
        config = DbtJobConfig(
            command="dbt run",
            repo=RepoConfig(url="https://github.com/org/repo"),
            connection=ConnectionConfig(
                lakehouse_name="lh",
                lakehouse_id="id1",
                workspace_id="ws1",
            ),
        )
        assert config.commands == ["dbt run"]

    def test_multi_command_list_preserved(self):
        config = DbtJobConfig(
            command=["dbt deps", "dbt run"],
            repo=RepoConfig(url="https://github.com/org/repo"),
            connection=ConnectionConfig(
                lakehouse_name="lh",
                lakehouse_id="id1",
                workspace_id="ws1",
            ),
        )
        assert config.commands == ["dbt deps", "dbt run"]


# ---------------------------------------------------------------------------
# _derive_job_name tests
# ---------------------------------------------------------------------------


class TestDeriveJobName:
    def test_derive_job_name_simple(self):
        assert _derive_job_name("dbt test") == "dbt_test"

    def test_derive_job_name_with_selector(self):
        assert _derive_job_name("dbt run --select tag:orders") == "dbt_run_tag_orders"

    def test_derive_job_name_with_short_selector(self):
        assert _derive_job_name("dbt run -s tag:orders") == "dbt_run_tag_orders"


# ---------------------------------------------------------------------------
# setup_environment tests
# ---------------------------------------------------------------------------


class TestSetupEnvironment:
    def test_setup_environment_sets_env_vars(self):
        import os

        conn = ConnectionConfig(
            lakehouse_name="my_lakehouse",
            lakehouse_id="lh-123",
            workspace_id="ws-456",
            workspace_name="my_workspace",
            schema_name="custom_schema",
        )
        with mock.patch.dict(os.environ, {}, clear=True):
            setup_environment(conn, "dbt_run")

            assert os.environ["LAKEHOUSE"] == "my_lakehouse"
            assert os.environ["LAKEHOUSE_ID"] == "lh-123"
            assert os.environ["SCHEMA"] == "custom_schema"
            assert os.environ["WORKSPACE_ID"] == "ws-456"
            assert os.environ["WORKSPACE_NAME"] == "my_workspace"
            assert os.environ["DBT_JOB_NAME"] == "dbt_run"

    def test_setup_environment_defaults(self):
        import os

        conn = ConnectionConfig(
            lakehouse_name="lh",
            lakehouse_id="id1",
            workspace_id="ws1",
        )
        with mock.patch.dict(os.environ, {}, clear=True):
            setup_environment(conn, "dbt_test")

            assert os.environ["SCHEMA"] == "dbo"
            assert os.environ["WORKSPACE_NAME"] == ""


# ---------------------------------------------------------------------------
# clone_repo tests
# ---------------------------------------------------------------------------


class TestCloneRepo:
    def _make_repo(self, **overrides) -> RepoConfig:
        defaults = dict(url="https://github.com/org/repo", branch="main")
        defaults.update(overrides)
        return RepoConfig(**defaults)

    @mock.patch("dbt.adapters.fabricspark.notebook.repo.shutil.rmtree")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.os.makedirs")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.subprocess.run")
    @mock.patch(
        "dbt.adapters.fabricspark.notebook.repo._resolve_token",
        return_value="abc",
    )
    def test_clone_with_pre_resolved_token(
        self, mock_resolve, mock_run, mock_makedirs, mock_rmtree
    ):
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )
        repo = self._make_repo(token="abc")
        clone_repo(repo, clone_dir="/tmp/test_clone")

        # The clone URL should contain the token
        call_args = mock_run.call_args[0][0]
        assert "x-access-token:abc@" in " ".join(call_args)

    @mock.patch("dbt.adapters.fabricspark.notebook.repo.shutil.rmtree")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.os.makedirs")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.subprocess.run")
    @mock.patch(
        "dbt.adapters.fabricspark.notebook.repo._resolve_token",
        return_value="",
    )
    def test_clone_public_repo_no_credentials(
        self, mock_resolve, mock_run, mock_makedirs, mock_rmtree
    ):
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )
        repo = self._make_repo()
        clone_repo(repo, clone_dir="/tmp/test_clone")

        # The clone URL should be plain (no token embedded)
        call_args = mock_run.call_args[0][0]
        assert "x-access-token" not in " ".join(call_args)
        assert "https://github.com/org/repo" in " ".join(call_args)

    @mock.patch("dbt.adapters.fabricspark.notebook.repo.shutil.rmtree")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.os.makedirs")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.subprocess.run")
    @mock.patch(
        "dbt.adapters.fabricspark.notebook.repo._resolve_token",
        return_value="",
    )
    def test_clone_fails_private_repo_no_auth(
        self, mock_resolve, mock_run, mock_makedirs, mock_rmtree
    ):
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=128, stdout="", stderr="fatal: could not read"
        )
        repo = self._make_repo()
        with pytest.raises(RuntimeError, match="provide GitHub App credentials"):
            clone_repo(repo, clone_dir="/tmp/test_clone")

    @mock.patch("dbt.adapters.fabricspark.notebook.repo.shutil.rmtree")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.os.makedirs")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.subprocess.run")
    @mock.patch(
        "dbt.adapters.fabricspark.notebook.repo._resolve_token",
        return_value="tok123",
    )
    def test_clone_fails_with_auth(
        self, mock_resolve, mock_run, mock_makedirs, mock_rmtree
    ):
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=128, stdout="", stderr="fatal: repo not found"
        )
        repo = self._make_repo(token="tok123")
        with pytest.raises(RuntimeError, match="git clone failed with authenticated URL"):
            clone_repo(repo, clone_dir="/tmp/test_clone")

    def test_clone_empty_url_raises(self):
        repo = self._make_repo(url="")
        with pytest.raises(ValueError, match="repo.url is required"):
            clone_repo(repo, clone_dir="/tmp/test_clone")

    @mock.patch("dbt.adapters.fabricspark.notebook.repo.shutil.rmtree")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.os.makedirs")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.subprocess.run")
    @mock.patch(
        "dbt.adapters.fabricspark.notebook.repo._resolve_token",
        return_value="",
    )
    def test_clone_with_branch(
        self, mock_resolve, mock_run, mock_makedirs, mock_rmtree
    ):
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )
        repo = self._make_repo(branch="dev")
        clone_repo(repo, clone_dir="/tmp/test_clone")

        call_args = mock_run.call_args[0][0]
        assert "-b" in call_args
        assert "dev" in call_args

    @mock.patch("dbt.adapters.fabricspark.notebook.repo.subprocess.run")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.os.makedirs")
    @mock.patch("dbt.adapters.fabricspark.notebook.repo.shutil.rmtree")
    @mock.patch(
        "dbt.adapters.fabricspark.notebook.repo._resolve_token",
        return_value="",
    )
    def test_clone_cleans_previous_directory(
        self, mock_resolve, mock_rmtree, mock_makedirs, mock_run
    ):
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )
        repo = self._make_repo()
        clone_repo(repo, clone_dir="/tmp/test_clone")

        mock_rmtree.assert_called_once_with("/tmp/test_clone", ignore_errors=True)


# ---------------------------------------------------------------------------
# _resolve_token tests
# ---------------------------------------------------------------------------


class TestResolveToken:
    def test_resolve_token_pre_resolved(self):
        repo = RepoConfig(url="https://github.com/org/repo", token="abc")
        assert _resolve_token(repo) == "abc"

    def test_resolve_token_no_credentials(self):
        repo = RepoConfig(url="https://github.com/org/repo")
        assert _resolve_token(repo) == ""

    def test_resolve_token_partial_credentials(self):
        repo = RepoConfig(
            url="https://github.com/org/repo",
            github_app_id="app123",
            # missing installation_id, pem_secret, vault_url
        )
        assert _resolve_token(repo) == ""


# ---------------------------------------------------------------------------
# run_dbt_job integration tests (mock everything)
# ---------------------------------------------------------------------------


class TestRunDbtJob:
    def _make_config(self, command="dbt run") -> DbtJobConfig:
        return DbtJobConfig(
            command=command,
            repo=RepoConfig(url="https://github.com/org/repo"),
            connection=ConnectionConfig(
                lakehouse_name="lh",
                lakehouse_id="id1",
                workspace_id="ws1",
            ),
        )

    @mock.patch("dbt.adapters.fabricspark.notebook.run_dbt")
    @mock.patch("dbt.adapters.fabricspark.notebook.clone_repo", return_value="/tmp/dbt_project")
    @mock.patch("dbt.adapters.fabricspark.notebook.setup_environment")
    def test_run_dbt_job_single_command(
        self, mock_setup, mock_clone, mock_run_dbt
    ):
        mock_run_dbt.return_value = DbtResult(
            success=True,
            exit_code=0,
            duration_ms=100,
            stdout="ok",
            stderr="",
            invocation_id="inv-1",
            log_dir="/logs",
        )

        config = self._make_config("dbt run")
        result = run_dbt_job(config)

        mock_setup.assert_called_once()
        mock_clone.assert_called_once()
        mock_run_dbt.assert_called_once_with("dbt run", "/tmp/dbt_project")
        assert result.success is True

    @mock.patch("dbt.adapters.fabricspark.notebook.run_dbt")
    @mock.patch("dbt.adapters.fabricspark.notebook.clone_repo", return_value="/tmp/dbt_project")
    @mock.patch("dbt.adapters.fabricspark.notebook.setup_environment")
    def test_run_dbt_job_multi_command(
        self, mock_setup, mock_clone, mock_run_dbt
    ):
        mock_run_dbt.return_value = DbtResult(
            success=True,
            exit_code=0,
            duration_ms=50,
            stdout="ok",
            stderr="",
            invocation_id="inv-2",
            log_dir="/logs",
        )

        config = self._make_config(["dbt deps", "dbt run"])
        run_dbt_job(config)

        assert mock_run_dbt.call_count == 2
        mock_run_dbt.assert_any_call("dbt deps", "/tmp/dbt_project")
        mock_run_dbt.assert_any_call("dbt run", "/tmp/dbt_project")

    @mock.patch("dbt.adapters.fabricspark.notebook.run_dbt")
    @mock.patch("dbt.adapters.fabricspark.notebook.clone_repo", return_value="/tmp/dbt_project")
    @mock.patch("dbt.adapters.fabricspark.notebook.setup_environment")
    def test_run_dbt_job_returns_last_result(
        self, mock_setup, mock_clone, mock_run_dbt
    ):
        first_result = DbtResult(
            success=True,
            exit_code=0,
            duration_ms=50,
            stdout="deps ok",
            stderr="",
            invocation_id="inv-first",
            log_dir="/logs",
        )
        last_result = DbtResult(
            success=True,
            exit_code=0,
            duration_ms=200,
            stdout="run ok",
            stderr="",
            invocation_id="inv-last",
            log_dir="/logs",
        )
        mock_run_dbt.side_effect = [first_result, last_result]

        config = self._make_config(["dbt deps", "dbt run"])
        result = run_dbt_job(config)

        assert result is last_result
        assert result.invocation_id == "inv-last"


# ---------------------------------------------------------------------------
# ConnectionConfig tests
# ---------------------------------------------------------------------------


class TestConnectionConfig:
    def test_connection_config_defaults(self):
        conn = ConnectionConfig(
            lakehouse_name="lh",
            lakehouse_id="id1",
            workspace_id="ws1",
        )
        assert conn.workspace_name == ""
        assert conn.schema_name == "dbo"
