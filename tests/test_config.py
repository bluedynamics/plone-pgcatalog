"""Tests for plone.pgcatalog.config — DSN discovery."""

import os
from unittest import mock

from plone.pgcatalog.config import get_dsn, _dsn_from_storage


class TestGetDsn:

    def test_env_var_highest_priority(self):
        with mock.patch.dict(os.environ, {"PGCATALOG_DSN": "host=myhost dbname=test"}):
            assert get_dsn() == "host=myhost dbname=test"

    def test_env_var_overrides_storage(self):
        site = mock.Mock()
        site._p_jar.db().storage._dsn = "from_storage"
        with mock.patch.dict(os.environ, {"PGCATALOG_DSN": "from_env"}):
            assert get_dsn(site) == "from_env"

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

    def test_returns_none_for_site_without_dsn(self):
        site = mock.Mock(spec=[])  # no _p_jar
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PGCATALOG_DSN", None)
            assert get_dsn(site) is None


class TestDsnFromStorage:

    def test_extracts_dsn(self):
        site = mock.Mock()
        site._p_jar.db().storage._dsn = "host=localhost dbname=zodb"
        assert _dsn_from_storage(site) == "host=localhost dbname=zodb"

    def test_returns_none_for_no_dsn_attr(self):
        site = mock.Mock()
        del site._p_jar.db().storage._dsn
        assert _dsn_from_storage(site) is None

    def test_returns_none_on_attribute_error(self):
        site = mock.Mock(spec=[])
        assert _dsn_from_storage(site) is None
