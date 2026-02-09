"""Tests for plone.pgcatalog.setuphandlers — GenericSetup install handler."""

from unittest import mock

from plone.pgcatalog.setuphandlers import install, _get_pg_connection


class TestInstall:

    def test_skips_without_sentinel_file(self):
        context = mock.Mock()
        context.readDataFile.return_value = None
        install(context)
        context.getSite.assert_not_called()

    def test_warns_without_pg_connection(self):
        context = mock.Mock()
        context.readDataFile.return_value = "sentinel"
        site = mock.Mock(spec=[])
        context.getSite.return_value = site
        with mock.patch("plone.pgcatalog.setuphandlers.log") as log_mock:
            install(context)
            log_mock.warning.assert_called_once()

    def test_installs_schema(self):
        context = mock.Mock()
        context.readDataFile.return_value = "sentinel"
        mock_conn = mock.Mock()
        site = mock.Mock()
        context.getSite.return_value = site
        with mock.patch(
            "plone.pgcatalog.setuphandlers._get_pg_connection",
            return_value=mock_conn,
        ), mock.patch(
            "plone.pgcatalog.setuphandlers.install_catalog_schema"
        ) as schema_mock:
            install(context)
            schema_mock.assert_called_once_with(mock_conn)
            mock_conn.commit.assert_called_once()


class TestGetPgConnection:

    def test_returns_connection_from_storage(self):
        site = mock.Mock()
        site._p_jar.db().storage._dsn = "host=localhost dbname=zodb port=5433"
        mock_conn = mock.Mock()
        with mock.patch("psycopg.connect", return_value=mock_conn):
            result = _get_pg_connection(site)
            assert result is mock_conn

    def test_returns_none_without_storage(self):
        site = mock.Mock(spec=[])
        assert _get_pg_connection(site) is None

    def test_returns_none_on_exception(self):
        site = mock.Mock()
        site._p_jar.db.side_effect = RuntimeError("oops")
        assert _get_pg_connection(site) is None
