"""Tests for concurrent Livy session creation.

Verifies that both the in-process _session_lock AND the cross-process
file lock (cross_process_session_lock) prevent duplicate session creation
-- the bug that caused 7 parallel `dbt show` sub-agents to each spin up a
separate Spark/Livy session on 2026-04-13.
"""
import multiprocessing
import os
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import dbt.adapters.fabricspark.livysession as livysession_module
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livysession import (
    LivySession,
    LivySessionManager,
    _session_lock,
    cross_process_session_lock,
    read_session_id_from_file,
    write_session_id_to_file,
)


def _make_local_credentials(**overrides):
    """Helper to create local-mode credentials for tests."""
    defaults = dict(
        method="livy",
        livy_mode="local",
        livy_url="http://localhost:8998",
        spark_config={"name": "test-session"},
    )
    defaults.update(overrides)
    return FabricSparkCredentials(**defaults)


def _make_fabric_credentials(**overrides):
    """Helper to create Fabric-mode credentials for tests."""
    defaults = dict(
        method="livy",
        livy_mode="fabric",
        authentication="CLI",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        lakehouse="tests",
        endpoint="https://api.fabric.microsoft.com/v1",
        spark_config={"name": "test-session"},
    )
    defaults.update(overrides)
    return FabricSparkCredentials(**defaults)


class TestSingleSessionForConcurrentThreads:
    """Launch 7 threads that all call LivySessionManager.connect() at the same
    time.  Assert that create_session is called exactly ONCE.

    After the first thread creates the session, subsequent threads must see it
    as valid via is_valid_session() and take the reuse fast-path.
    """

    def setup_method(self):
        LivySessionManager.livy_global_session = None

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_single_session_created_for_concurrent_threads_local(
        self, mock_create, mock_valid, mock_headers
    ):
        mock_headers.return_value = {"Content-Type": "application/json"}

        # Simulate slow session creation so all threads queue up behind the lock
        def slow_create(config):
            time.sleep(0.1)
            LivySessionManager.livy_global_session.session_id = "shared-1"
            LivySessionManager.livy_global_session.is_new_session_required = False

        mock_create.side_effect = slow_create

        credentials = _make_local_credentials()
        num_threads = 7
        barrier = threading.Barrier(num_threads)
        errors = []

        def connect_thread():
            try:
                barrier.wait(timeout=5)
                LivySessionManager.connect(credentials)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=connect_thread) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        assert not errors, f"Threads raised errors: {errors}"
        assert mock_create.call_count == 1, (
            f"Expected create_session to be called once, but was called "
            f"{mock_create.call_count} times"
        )

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_single_session_created_for_concurrent_threads_fabric(
        self, mock_create, mock_valid, mock_headers
    ):
        mock_headers.return_value = {"Content-Type": "application/json"}

        def slow_create(config):
            time.sleep(0.1)
            LivySessionManager.livy_global_session.session_id = "fab-shared-1"
            LivySessionManager.livy_global_session.is_new_session_required = False

        mock_create.side_effect = slow_create

        credentials = _make_fabric_credentials()
        num_threads = 7
        barrier = threading.Barrier(num_threads)
        errors = []

        def connect_thread():
            try:
                barrier.wait(timeout=5)
                LivySessionManager.connect(credentials)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=connect_thread) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        assert not errors, f"Threads raised errors: {errors}"
        assert mock_create.call_count == 1, (
            f"Expected create_session to be called once, but was called "
            f"{mock_create.call_count} times"
        )


class TestSessionLockPreventsDuplicateCreation:
    """Verify that _session_lock serialises session creation: one thread holds
    the lock while creating the session; the others block and then reuse it."""

    def setup_method(self):
        LivySessionManager.livy_global_session = None

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_session_lock_prevents_duplicate_creation(
        self, mock_create, mock_valid, mock_headers
    ):
        mock_headers.return_value = {"Content-Type": "application/json"}

        creation_started = threading.Event()
        creation_proceed = threading.Event()
        waiters_saw_lock_held = []

        def slow_create_with_signal(config):
            creation_started.set()
            creation_proceed.wait(timeout=5)
            LivySessionManager.livy_global_session.session_id = "lock-test-1"
            LivySessionManager.livy_global_session.is_new_session_required = False

        mock_create.side_effect = slow_create_with_signal

        credentials = _make_local_credentials()
        errors = []

        def creator_thread():
            try:
                LivySessionManager.connect(credentials)
            except Exception as e:
                errors.append(e)

        def waiter_thread():
            try:
                # Wait until the creator has started creating
                creation_started.wait(timeout=5)
                # At this point, the creator holds _session_lock inside connect().
                # Try to acquire the lock with a short timeout to prove it is held.
                acquired = _session_lock.acquire(timeout=0.05)
                if acquired:
                    _session_lock.release()
                    waiters_saw_lock_held.append(False)
                else:
                    waiters_saw_lock_held.append(True)
                # Now actually connect (will block until creator finishes)
                LivySessionManager.connect(credentials)
            except Exception as e:
                errors.append(e)

        t_creator = threading.Thread(target=creator_thread)
        t_waiter1 = threading.Thread(target=waiter_thread)
        t_waiter2 = threading.Thread(target=waiter_thread)

        t_creator.start()
        t_waiter1.start()
        t_waiter2.start()

        # Let the creation complete once waiters have had a chance to probe the lock
        creation_started.wait(timeout=5)
        time.sleep(0.15)  # Give waiters time to try the lock
        creation_proceed.set()

        t_creator.join(timeout=10)
        t_waiter1.join(timeout=10)
        t_waiter2.join(timeout=10)

        assert not errors, f"Threads raised errors: {errors}"
        # At least one waiter should have observed the lock was held
        assert any(waiters_saw_lock_held), (
            "Expected at least one waiter to observe that _session_lock was held "
            "during session creation"
        )
        # Only one creation should have happened
        assert mock_create.call_count == 1


class TestThreadLocalConnectionManagers:
    """Verify that FabricSparkConnectionManager.connection_managers uses thread
    IDs as keys, but all threads share the same underlying LivySession."""

    def setup_method(self):
        LivySessionManager.livy_global_session = None

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_thread_local_connection_managers(self, mock_create, mock_valid, mock_headers):
        """3 threads call open()-equivalent logic; they get separate
        connection_manager entries keyed by thread ID but the same global
        LivySession underneath."""
        from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager

        mock_headers.return_value = {"Content-Type": "application/json"}

        def create_side_effect(config):
            LivySessionManager.livy_global_session.session_id = "shared-42"
            LivySessionManager.livy_global_session.is_new_session_required = False

        mock_create.side_effect = create_side_effect

        credentials = _make_local_credentials()
        thread_ids_seen = []
        sessions_seen = []
        connection_managers_ref = FabricSparkConnectionManager.connection_managers
        # Clear state from other tests
        connection_managers_ref.clear()

        barrier = threading.Barrier(3)
        errors = []

        def thread_fn():
            try:
                barrier.wait(timeout=5)
                thread_id = FabricSparkConnectionManager.get_thread_identifier()
                if thread_id not in connection_managers_ref:
                    connection_managers_ref[thread_id] = LivySessionManager()
                conn = connection_managers_ref[thread_id].connect(credentials)
                thread_ids_seen.append(thread_id)
                # All connections should reference the same global session
                sessions_seen.append(LivySessionManager.livy_global_session)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=thread_fn) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        assert not errors, f"Threads raised errors: {errors}"
        # Each thread should have a unique thread_id key
        assert len(set(thread_ids_seen)) == 3, (
            f"Expected 3 unique thread IDs, got {thread_ids_seen}"
        )
        # All threads should see the same LivySession object
        assert all(s is sessions_seen[0] for s in sessions_seen), (
            "All threads should share the same underlying LivySession"
        )
        # Session should have been created only once
        assert mock_create.call_count == 1

        # Cleanup
        connection_managers_ref.clear()


class TestSessionReuseAcrossInvocations:
    """Create a LivySessionManager, connect once (creates session), connect
    again (reuses it).  Assert create_session called exactly once."""

    def setup_method(self):
        LivySessionManager.livy_global_session = None

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_session_reuse_across_invocations_local(
        self, mock_create, mock_valid, mock_headers
    ):
        mock_headers.return_value = {"Content-Type": "application/json"}

        def create_side_effect(config):
            LivySessionManager.livy_global_session.session_id = "persist-1"
            LivySessionManager.livy_global_session.is_new_session_required = False

        mock_create.side_effect = create_side_effect

        credentials = _make_local_credentials()

        # First call -- creates session
        LivySessionManager.connect(credentials)
        assert mock_create.call_count == 1

        # Second call -- should reuse (is_valid_session returns True, is_new_session_required is False)
        LivySessionManager.connect(credentials)
        assert mock_create.call_count == 1, (
            "create_session should not be called on second connect"
        )

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_session_reuse_across_invocations_fabric(
        self, mock_create, mock_valid, mock_headers
    ):
        mock_headers.return_value = {"Content-Type": "application/json"}

        def create_side_effect(config):
            LivySessionManager.livy_global_session.session_id = "fab-persist-1"
            LivySessionManager.livy_global_session.is_new_session_required = False

        mock_create.side_effect = create_side_effect

        credentials = _make_fabric_credentials()

        # First call -- creates session
        LivySessionManager.connect(credentials)
        assert mock_create.call_count == 1

        # Second call -- should reuse
        LivySessionManager.connect(credentials)
        assert mock_create.call_count == 1, (
            "create_session should not be called on second connect in Fabric mode"
        )


class TestConcurrentConnectWithReuseFlag:
    """With reuse_session=True and a valid session_id_file, concurrent threads
    should all reuse the persisted session (zero new sessions created)."""

    def setup_method(self):
        LivySessionManager.livy_global_session = None

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_concurrent_connect_with_reuse_flag(self, mock_create, mock_valid, mock_headers):
        mock_headers.return_value = {"Content-Type": "application/json"}

        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "session.txt")
            write_session_id_to_file(session_file, "persisted-77")

            credentials = _make_fabric_credentials(
                reuse_session=True,
                session_id_file=session_file,
            )

            num_threads = 5
            barrier = threading.Barrier(num_threads)
            errors = []

            with patch.object(
                LivySession, "try_reuse_session", return_value=True
            ) as mock_reuse:
                def connect_thread():
                    try:
                        barrier.wait(timeout=5)
                        LivySessionManager.connect(credentials)
                    except Exception as e:
                        errors.append(e)

                threads = [
                    threading.Thread(target=connect_thread)
                    for _ in range(num_threads)
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=15)

            assert not errors, f"Threads raised errors: {errors}"
            # create_session should never have been called -- all threads reuse
            assert mock_create.call_count == 0, (
                f"Expected 0 create_session calls (all should reuse), "
                f"got {mock_create.call_count}"
            )


class TestSessionNotDuplicatedUnderRaceCondition:
    """Simulate a race: thread 1 starts creating a session (slow mock),
    threads 2-7 arrive while thread 1 is still creating.  All should wait
    and then reuse the same session -- only 1 create_session call total."""

    def setup_method(self):
        LivySessionManager.livy_global_session = None

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_session_not_duplicated_even_under_race_condition_local(
        self, mock_create, mock_valid, mock_headers
    ):
        mock_headers.return_value = {"Content-Type": "application/json"}

        creation_event = threading.Event()

        def slow_create(config):
            # Signal that creation has started
            creation_event.set()
            # Simulate a 300ms session start delay
            time.sleep(0.3)
            LivySessionManager.livy_global_session.session_id = "race-local-1"
            LivySessionManager.livy_global_session.is_new_session_required = False

        mock_create.side_effect = slow_create

        credentials = _make_local_credentials()
        num_threads = 7
        errors = []
        session_ids = []

        def first_thread():
            try:
                conn = LivySessionManager.connect(credentials)
                session_ids.append(conn.session_id)
            except Exception as e:
                errors.append(e)

        def follower_thread():
            try:
                # Wait until the first thread has begun creating
                creation_event.wait(timeout=5)
                # Small delay to ensure we arrive while creation is in progress
                time.sleep(0.05)
                conn = LivySessionManager.connect(credentials)
                session_ids.append(conn.session_id)
            except Exception as e:
                errors.append(e)

        t_first = threading.Thread(target=first_thread)
        t_followers = [
            threading.Thread(target=follower_thread) for _ in range(num_threads - 1)
        ]

        t_first.start()
        for t in t_followers:
            t.start()

        t_first.join(timeout=15)
        for t in t_followers:
            t.join(timeout=15)

        assert not errors, f"Threads raised errors: {errors}"
        assert mock_create.call_count == 1, (
            f"Expected 1 create_session call under race, got {mock_create.call_count}"
        )
        # All threads should have gotten the same session ID
        assert all(sid == "race-local-1" for sid in session_ids), (
            f"Expected all threads to see session 'race-local-1', got {session_ids}"
        )

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_session_not_duplicated_even_under_race_condition_fabric(
        self, mock_create, mock_valid, mock_headers
    ):
        mock_headers.return_value = {"Content-Type": "application/json"}

        creation_event = threading.Event()

        def slow_create(config):
            creation_event.set()
            time.sleep(0.3)
            LivySessionManager.livy_global_session.session_id = "race-fabric-1"
            LivySessionManager.livy_global_session.is_new_session_required = False

        mock_create.side_effect = slow_create

        credentials = _make_fabric_credentials()
        num_threads = 7
        errors = []
        session_ids = []

        def first_thread():
            try:
                conn = LivySessionManager.connect(credentials)
                session_ids.append(conn.session_id)
            except Exception as e:
                errors.append(e)

        def follower_thread():
            try:
                creation_event.wait(timeout=5)
                time.sleep(0.05)
                conn = LivySessionManager.connect(credentials)
                session_ids.append(conn.session_id)
            except Exception as e:
                errors.append(e)

        t_first = threading.Thread(target=first_thread)
        t_followers = [
            threading.Thread(target=follower_thread) for _ in range(num_threads - 1)
        ]

        t_first.start()
        for t in t_followers:
            t.start()

        t_first.join(timeout=15)
        for t in t_followers:
            t.join(timeout=15)

        assert not errors, f"Threads raised errors: {errors}"
        assert mock_create.call_count == 1, (
            f"Expected 1 create_session call under race, got {mock_create.call_count}"
        )
        assert all(sid == "race-fabric-1" for sid in session_ids), (
            f"Expected all threads to see session 'race-fabric-1', got {session_ids}"
        )


# ── Cross-process file lock tests ──────────────────────────────────────
# These test the cross_process_session_lock() context manager that
# prevents parallel dbt PROCESSES (not just threads) from each creating
# their own Spark session.


class TestCrossProcessSessionLock:
    """Test the fcntl.flock-based cross-process session lock."""

    def test_lock_creates_lock_file(self):
        """Lock file is created next to the session file."""
        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "livy-session-id.txt")
            lock_file = session_file + ".lock"

            assert not os.path.exists(lock_file)
            with cross_process_session_lock(session_file):
                assert os.path.exists(lock_file)

    def test_lock_is_exclusive(self):
        """A second attempt to acquire the lock blocks (non-blocking check)."""
        import fcntl

        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "livy-session-id.txt")
            lock_file = session_file + ".lock"

            with cross_process_session_lock(session_file):
                # While we hold the lock, try a non-blocking acquire
                fd = open(lock_file, "w")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except (OSError, BlockingIOError):
                    acquired = False
                finally:
                    fd.close()

                assert not acquired, "Lock should be held; second acquire should fail"

    def test_lock_released_after_context_exit(self):
        """Lock is released when the context manager exits."""
        import fcntl

        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "livy-session-id.txt")
            lock_file = session_file + ".lock"

            with cross_process_session_lock(session_file):
                pass  # Lock held here

            # Lock should be released now
            fd = open(lock_file, "w")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                fcntl.flock(fd, fcntl.LOCK_UN)
            except (OSError, BlockingIOError):
                acquired = False
            finally:
                fd.close()

            assert acquired, "Lock should be released after context exit"

    def test_second_process_waits_then_reuses_session(self):
        """Simulate two processes: P1 creates session, P2 waits and reuses.

        Uses multiprocessing to prove the file lock works across processes,
        not just threads.
        """
        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "livy-session-id.txt")
            result_file = os.path.join(tmp, "p2_result.txt")

            def process_1():
                """Acquire lock, create session, write ID, hold lock briefly."""
                with cross_process_session_lock(session_file):
                    # Simulate slow session creation
                    time.sleep(1)
                    write_session_id_to_file(session_file, "shared-session-42")
                    # Hold lock a bit longer so P2 has to wait
                    time.sleep(0.5)

            def process_2():
                """Wait for lock, then read the session file."""
                # Small delay so P1 grabs lock first
                time.sleep(0.2)
                with cross_process_session_lock(session_file):
                    session_id = read_session_id_from_file(session_file)
                    with open(result_file, "w") as f:
                        f.write(session_id or "NONE")

            p1 = multiprocessing.Process(target=process_1)
            p2 = multiprocessing.Process(target=process_2)

            p1.start()
            p2.start()
            p1.join(timeout=15)
            p2.join(timeout=15)

            assert p1.exitcode == 0, f"P1 failed with exit code {p1.exitcode}"
            assert p2.exitcode == 0, f"P2 failed with exit code {p2.exitcode}"

            # P2 should have read the session ID written by P1
            with open(result_file) as f:
                p2_session_id = f.read().strip()
            assert p2_session_id == "shared-session-42", (
                f"P2 should see P1's session, got: {p2_session_id}"
            )
