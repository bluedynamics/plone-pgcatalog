"""Tests for plone.pgcatalog.config — pool discovery, DSN fallback, connection reuse."""

from plone.pgcatalog.config import _pool_from_storage
from plone.pgcatalog.config import get_dsn
from plone.pgcatalog.config import get_pool
from plone.pgcatalog.config import get_request_connection
from plone.pgcatalog.config import release_request_connection
from unittest import mock

import os
import plone.pgcatalog.config as config_mod
import pytest


class TestGetPool:

    def test_returns_pool_from_storage(self):
        mock_pool = mock.Mock()
        site = mock.Mock()
        site._p_jar.db().storage._instance_pool = mock_pool
        assert get_pool(site) is mock_pool

    def test_falls_back_to_env_pool(self):
        mock_pool = mock.Mock()
        with mock.patch.dict(os.environ, {"PGCATALOG_DSN": "host=test dbname=test"}), \
             mock.patch("psycopg_pool.ConnectionPool", return_value=mock_pool):
            config_mod._fallback_pool = None
            try:
                pool = get_pool()
                assert pool is mock_pool
            finally:
                config_mod._fallback_pool = None

    def test_storage_takes_priority_over_env(self):
        mock_pool = mock.Mock()
        site = mock.Mock()
        site._p_jar.db().storage._instance_pool = mock_pool
        with mock.patch.dict(os.environ, {"PGCATALOG_DSN": "host=test"}):
            assert get_pool(site) is mock_pool

    def test_raises_without_any_source(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PGCATALOG_DSN", None)
            config_mod._fallback_pool = None
            with pytest.raises(RuntimeError, match="Cannot find PG connection pool"):
                get_pool()

    def test_raises_for_site_without_pool(self):
        site = mock.Mock(spec=[])  # no _p_jar
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PGCATALOG_DSN", None)
            config_mod._fallback_pool = None
            with pytest.raises(RuntimeError, match="Cannot find PG connection pool"):
                get_pool(site)


class TestPoolFromStorage:

    def test_extracts_pool(self):
        mock_pool = mock.Mock()
        site = mock.Mock()
        site._p_jar.db().storage._instance_pool = mock_pool
        assert _pool_from_storage(site) is mock_pool

    def test_returns_none_for_no_pool_attr(self):
        site = mock.Mock()
        del site._p_jar.db().storage._instance_pool
        assert _pool_from_storage(site) is None

    def test_returns_none_on_attribute_error(self):
        site = mock.Mock(spec=[])
        assert _pool_from_storage(site) is None


class TestGetDsn:
    """get_dsn is kept for setuphandlers.py backward compat."""

    def test_env_var_highest_priority(self):
        with mock.patch.dict(os.environ, {"PGCATALOG_DSN": "host=myhost dbname=test"}):
            assert get_dsn() == "host=myhost dbname=test"

    def test_from_storage(self):
        site = mock.Mock()
        site._p_jar.db().storage._dsn = "host=pg dbname=zodb"
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PGCATALOG_DSN", None)
            assert get_dsn(site) == "host=pg dbname=zodb"

    def test_returns_none_when_no_source(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PGCATALOG_DSN", None)
            assert get_dsn() is None


class TestRequestConnection:
    """Request-scoped connection reuse (Phase 4)."""

    def setup_method(self):
        """Clean thread-local state before each test."""
        config_mod._local.pgcat_conn = None
        config_mod._local.pgcat_pool = None

    def teardown_method(self):
        """Clean thread-local state after each test."""
        config_mod._local.pgcat_conn = None
        config_mod._local.pgcat_pool = None

    def test_creates_conn_on_first_call(self):
        pool = mock.Mock()
        conn = mock.Mock()
        pool.getconn.return_value = conn

        result = get_request_connection(pool)
        assert result is conn
        pool.getconn.assert_called_once()

    def test_reuses_conn_on_second_call(self):
        pool = mock.Mock()
        conn = mock.Mock()
        conn.closed = False
        pool.getconn.return_value = conn

        first = get_request_connection(pool)
        second = get_request_connection(pool)
        assert first is second
        # Only one getconn call
        pool.getconn.assert_called_once()

    def test_release_returns_conn_to_pool(self):
        pool = mock.Mock()
        conn = mock.Mock()
        conn.closed = False
        pool.getconn.return_value = conn

        get_request_connection(pool)
        release_request_connection()

        pool.putconn.assert_called_once_with(conn)

    def test_release_clears_thread_local(self):
        pool = mock.Mock()
        conn = mock.Mock()
        conn.closed = False
        pool.getconn.return_value = conn

        get_request_connection(pool)
        release_request_connection()

        assert getattr(config_mod._local, "pgcat_conn", None) is None
        assert getattr(config_mod._local, "pgcat_pool", None) is None

    def test_release_is_noop_when_no_conn(self):
        # Should not raise
        release_request_connection()

    def test_new_conn_after_release(self):
        pool = mock.Mock()
        conn1 = mock.Mock()
        conn1.closed = False
        conn2 = mock.Mock()
        conn2.closed = False
        pool.getconn.side_effect = [conn1, conn2]

        first = get_request_connection(pool)
        release_request_connection()
        second = get_request_connection(pool)

        assert first is conn1
        assert second is conn2
        assert pool.getconn.call_count == 2

    def test_creates_new_conn_if_closed(self):
        pool = mock.Mock()
        conn1 = mock.Mock()
        conn1.closed = True
        conn2 = mock.Mock()
        conn2.closed = False
        pool.getconn.side_effect = [conn1, conn2]

        get_request_connection(pool)
        # conn1 is closed, should get new one
        result = get_request_connection(pool)
        assert result is conn2


class TestOrjsonLoader:
    """Phase 1: orjson JSONB loader."""

    def test_orjson_is_installed(self):
        import orjson

        # orjson.loads returns bytes → same Python types as json.loads
        result = orjson.loads(b'{"key": "value"}')
        assert result == {"key": "value"}
