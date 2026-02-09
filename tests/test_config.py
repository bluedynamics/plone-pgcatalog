"""Tests for plone.pgcatalog.config — pool discovery and DSN fallback."""

from plone.pgcatalog.config import _pool_from_storage
from plone.pgcatalog.config import get_dsn
from plone.pgcatalog.config import get_pool
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
