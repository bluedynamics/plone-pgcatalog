"""Tests for plone.pgcatalog.config — pool discovery, DSN fallback, connection reuse."""

from plone.pgcatalog.config import _pool_from_storage
from plone.pgcatalog.config import get_dsn
from plone.pgcatalog.config import get_pool
from plone.pgcatalog.config import get_request_connection
from plone.pgcatalog.config import get_storage_connection
from plone.pgcatalog.config import release_request_connection
from unittest import mock

import os
import plone.pgcatalog.config as config_mod
import pytest
import transaction


class TestGetStorageConnection:
    def test_returns_pg_connection_from_storage(self):
        mock_conn = mock.Mock()
        context = mock.Mock()
        context._p_jar._normal_storage.pg_connection = mock_conn
        assert get_storage_connection(context) is mock_conn

    def test_falls_back_to_storage_without_normal_storage(self):
        mock_conn = mock.Mock()
        context = mock.Mock()
        context._p_jar._normal_storage = None
        context._p_jar._storage.pg_connection = mock_conn
        assert get_storage_connection(context) is mock_conn

    def test_returns_none_without_p_jar(self):
        context = mock.Mock(spec=[])  # no _p_jar
        assert get_storage_connection(context) is None

    def test_returns_none_without_pg_connection_attr(self):
        context = mock.Mock()
        context._p_jar._normal_storage = None
        del context._p_jar._storage.pg_connection
        assert get_storage_connection(context) is None

    def test_returns_none_on_attribute_error(self):
        context = mock.Mock(spec=[])  # no attributes at all
        assert get_storage_connection(context) is None


class TestGetPool:
    def test_returns_pool_from_storage(self):
        mock_pool = mock.Mock()
        site = mock.Mock()
        site._p_jar.db().storage._instance_pool = mock_pool
        assert get_pool(site) is mock_pool

    def test_falls_back_to_env_pool(self):
        mock_pool = mock.Mock()
        with (
            mock.patch.dict(os.environ, {"PGCATALOG_DSN": "host=test dbname=test"}),
            mock.patch("psycopg_pool.ConnectionPool", return_value=mock_pool),
        ):
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

    def test_returns_none_on_storage_error(self):
        site = mock.Mock(spec=[])  # no _p_jar
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PGCATALOG_DSN", None)
            assert get_dsn(site) is None


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

    def test_release_swallows_putconn_exception(self):
        """release_request_connection swallows exceptions from pool.putconn."""
        pool = mock.Mock()
        conn = mock.Mock()
        conn.closed = False
        pool.getconn.return_value = conn
        pool.putconn.side_effect = RuntimeError("pool closed")

        get_request_connection(pool)
        # Should not raise
        release_request_connection()
        # Thread-local still cleaned
        assert getattr(config_mod._local, "pgcat_conn", None) is None


def _clean_pending():
    """Clear thread-local pending state and abort current transaction."""
    try:
        del config_mod._local.pending
    except AttributeError:
        pass
    try:
        del config_mod._local._pending_dm
    except AttributeError:
        pass
    transaction.abort()


class TestPending:
    """Thread-local pending catalog data store."""

    def setup_method(self):
        _clean_pending()

    def teardown_method(self):
        _clean_pending()

    def test_set_and_pop_pending(self):
        from plone.pgcatalog.config import pop_pending
        from plone.pgcatalog.config import set_pending

        set_pending(42, {"path": "/plone/doc"})
        result = pop_pending(42)
        assert result == {"path": "/plone/doc"}

    def test_pop_missing_returns_sentinel(self):
        from plone.pgcatalog.config import _MISSING
        from plone.pgcatalog.config import pop_pending

        result = pop_pending(999)
        assert result is _MISSING

    def test_set_pending_uncatalog_sentinel(self):
        from plone.pgcatalog.config import pop_pending
        from plone.pgcatalog.config import set_pending

        set_pending(42, None)
        result = pop_pending(42)
        assert result is None

    def test_get_pending_creates_dict_on_fresh_thread_local(self):
        from plone.pgcatalog.config import _get_pending

        result = _get_pending()
        assert isinstance(result, dict)


class TestPendingSavepoint:
    """Savepoint-aware pending catalog data."""

    def setup_method(self):
        _clean_pending()

    def teardown_method(self):
        _clean_pending()

    def test_set_pending_joins_transaction(self):
        from plone.pgcatalog.config import PendingDataManager
        from plone.pgcatalog.config import set_pending

        txn = transaction.get()
        set_pending(1, {"path": "/doc1"})
        assert any(isinstance(r, PendingDataManager) for r in txn._resources)

    def test_set_pending_joins_only_once(self):
        from plone.pgcatalog.config import PendingDataManager
        from plone.pgcatalog.config import set_pending

        txn = transaction.get()
        set_pending(1, {"path": "/doc1"})
        set_pending(2, {"path": "/doc2"})
        dm_count = sum(1 for r in txn._resources if isinstance(r, PendingDataManager))
        assert dm_count == 1

    def test_abort_clears_pending(self):
        from plone.pgcatalog.config import _get_pending
        from plone.pgcatalog.config import set_pending

        set_pending(1, {"path": "/doc1"})
        transaction.abort()
        assert _get_pending() == {}

    def test_savepoint_rollback_restores_state(self):
        from plone.pgcatalog.config import _get_pending
        from plone.pgcatalog.config import set_pending

        set_pending(1, {"path": "/doc1"})
        sp = transaction.savepoint()
        set_pending(2, {"path": "/doc2"})
        set_pending(1, {"path": "/doc1-v2"})
        assert len(_get_pending()) == 2
        sp.rollback()
        pending = _get_pending()
        assert pending == {1: {"path": "/doc1"}}
        assert 2 not in pending

    def test_savepoint_rollback_to_empty(self):
        from plone.pgcatalog.config import _get_pending
        from plone.pgcatalog.config import set_pending

        sp = transaction.savepoint()
        set_pending(1, {"path": "/doc1"})
        # DM joined after savepoint → AbortSavepoint clears all
        sp.rollback()
        assert _get_pending() == {}

    def test_multiple_savepoints(self):
        from plone.pgcatalog.config import _get_pending
        from plone.pgcatalog.config import set_pending

        set_pending(1, {"path": "/doc1"})
        sp1 = transaction.savepoint()
        set_pending(2, {"path": "/doc2"})
        sp2 = transaction.savepoint()
        set_pending(3, {"path": "/doc3"})
        sp2.rollback()
        assert set(_get_pending().keys()) == {1, 2}
        sp1.rollback()
        assert set(_get_pending().keys()) == {1}

    def test_new_transaction_gets_new_dm(self):
        from plone.pgcatalog.config import set_pending

        set_pending(1, {"path": "/doc1"})
        dm1 = config_mod._local._pending_dm
        transaction.abort()
        set_pending(2, {"path": "/doc2"})
        dm2 = config_mod._local._pending_dm
        assert dm1 is not dm2

    def test_uncatalog_sentinel_survives_savepoint(self):
        from plone.pgcatalog.config import _get_pending
        from plone.pgcatalog.config import set_pending

        set_pending(1, None)
        sp = transaction.savepoint()
        set_pending(1, {"path": "/doc1"})
        sp.rollback()
        assert _get_pending() == {1: None}

    def test_rejoin_after_abort_savepoint(self):
        from plone.pgcatalog.config import _get_pending
        from plone.pgcatalog.config import PendingDataManager
        from plone.pgcatalog.config import set_pending

        sp = transaction.savepoint()
        set_pending(1, {"path": "/doc1"})
        sp.rollback()
        assert _get_pending() == {}
        # After AbortSavepoint unjoin, set_pending must rejoin
        set_pending(2, {"path": "/doc2"})
        txn = transaction.get()
        assert any(isinstance(r, PendingDataManager) for r in txn._resources)
        assert _get_pending() == {2: {"path": "/doc2"}}

    def test_tpc_finish_clears_pending(self):
        from plone.pgcatalog.config import _get_pending
        from plone.pgcatalog.config import set_pending

        set_pending(1, {"path": "/doc1"})
        dm = config_mod._local._pending_dm
        dm.tpc_finish(transaction.get())
        assert _get_pending() == {}

    def test_sort_key(self):
        from plone.pgcatalog.config import PendingDataManager

        dm = PendingDataManager(transaction.get())
        key = dm.sortKey()
        assert key == "~plone.pgcatalog.pending"
        assert key > "z"  # sorts after all alphanumeric

    def test_interface_compliance(self):
        from plone.pgcatalog.config import PendingDataManager
        from plone.pgcatalog.config import PendingSavepoint
        from transaction.interfaces import IDataManagerSavepoint
        from transaction.interfaces import ISavepointDataManager
        from zope.interface.verify import verifyObject

        dm = PendingDataManager(transaction.get())
        assert verifyObject(ISavepointDataManager, dm)
        sp = PendingSavepoint({})
        assert verifyObject(IDataManagerSavepoint, sp)


class TestPoolFromEnvCached:
    def test_returns_cached_pool(self):
        mock_pool = mock.Mock()
        config_mod._fallback_pool = mock_pool
        try:
            from plone.pgcatalog.config import _pool_from_env

            result = _pool_from_env()
            assert result is mock_pool
        finally:
            config_mod._fallback_pool = None


class TestCatalogStateProcessor:
    """Tests for CatalogStateProcessor."""

    def setup_method(self):
        _clean_pending()

    def teardown_method(self):
        _clean_pending()

    def test_get_extra_columns(self):
        from plone.pgcatalog.config import CatalogStateProcessor

        processor = CatalogStateProcessor()
        columns = processor.get_extra_columns()
        assert len(columns) == 3
        names = [c.name for c in columns]
        assert "path" in names
        assert "idx" in names
        assert "searchable_text" in names

    def test_get_schema_sql(self):
        from plone.pgcatalog.config import CatalogStateProcessor

        processor = CatalogStateProcessor()
        sql = processor.get_schema_sql()
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_process_returns_none_when_no_pending(self):
        from plone.pgcatalog.config import CatalogStateProcessor

        processor = CatalogStateProcessor()
        result = processor.process(999, "some.module", "SomeClass", {"key": "value"})
        assert result is None

    def test_process_with_pending_from_thread_local(self):
        from plone.pgcatalog.config import CatalogStateProcessor
        from plone.pgcatalog.config import set_pending

        set_pending(
            42,
            {
                "path": "/plone/doc",
                "idx": {"portal_type": "Document"},
                "searchable_text": "hello",
            },
        )
        processor = CatalogStateProcessor()
        result = processor.process(42, "some.module", "SomeClass", {})
        assert result["path"] == "/plone/doc"
        assert result["searchable_text"] == "hello"
        assert result["idx"] is not None

    def test_process_uncatalog_sentinel(self):
        from plone.pgcatalog.config import CatalogStateProcessor
        from plone.pgcatalog.config import set_pending

        set_pending(42, None)
        processor = CatalogStateProcessor()
        result = processor.process(42, "some.module", "SomeClass", {})
        assert result == {"path": None, "idx": None, "searchable_text": None}

    def test_process_from_state_dict_fallback(self):
        from plone.pgcatalog.config import ANNOTATION_KEY
        from plone.pgcatalog.config import CatalogStateProcessor

        state = {
            ANNOTATION_KEY: {
                "path": "/plone/doc2",
                "idx": {"portal_type": "File"},
                "searchable_text": "world",
            },
            "other_key": "other_value",
        }
        processor = CatalogStateProcessor()
        result = processor.process(888, "some.module", "SomeClass", state)
        assert result["path"] == "/plone/doc2"
        # annotation key should be popped from state
        assert ANNOTATION_KEY not in state
        assert state["other_key"] == "other_value"

    def test_process_with_empty_idx(self):
        from plone.pgcatalog.config import CatalogStateProcessor
        from plone.pgcatalog.config import set_pending

        set_pending(42, {"path": "/plone/doc", "idx": {}, "searchable_text": None})
        processor = CatalogStateProcessor()
        result = processor.process(42, "mod", "Cls", {})
        assert result["path"] == "/plone/doc"
        assert result["idx"] is None  # empty dict → None
        assert result["searchable_text"] is None


class TestGetMainStorage:
    def test_returns_storage_directly(self):
        from plone.pgcatalog.config import _get_main_storage

        db = mock.Mock()
        storage = mock.Mock(spec=[])  # no _main attr
        db.storage = storage
        result = _get_main_storage(db)
        assert result is storage

    def test_unwraps_main_storage(self):
        from plone.pgcatalog.config import _get_main_storage

        db = mock.Mock()
        main_storage = mock.Mock()
        db.storage._main = main_storage
        result = _get_main_storage(db)
        assert result is main_storage


class TestRegisterCatalogProcessor:
    def test_registers_on_pgjsonb_storage(self):
        from plone.pgcatalog.config import register_catalog_processor

        event = mock.Mock()
        storage = mock.Mock()
        storage.register_state_processor = mock.Mock()
        event.database.storage = storage
        del storage._main  # no wrapper

        with mock.patch("plone.pgcatalog.config._sync_registry_from_db"):
            register_catalog_processor(event)

        storage.register_state_processor.assert_called_once()

    def test_skips_non_pgjsonb_storage(self):
        from plone.pgcatalog.config import register_catalog_processor

        event = mock.Mock()
        storage = mock.Mock(spec=[])  # no register_state_processor
        event.database.storage = storage

        # Should not raise
        register_catalog_processor(event)


class TestSyncRegistryFromDb:
    def test_syncs_from_plone_site(self):
        from plone.pgcatalog.config import _sync_registry_from_db

        db = mock.Mock()
        conn = mock.Mock()
        db.open.return_value = conn

        site = mock.Mock()
        site.getId.return_value = "Plone"
        catalog = mock.Mock()
        catalog._catalog = mock.Mock()
        site.portal_catalog = catalog

        root = mock.Mock()
        root.get.return_value = root
        root.values.return_value = [site]
        conn.root.return_value = root

        with mock.patch("plone.pgcatalog.columns.get_registry") as get_reg:
            registry = mock.Mock()
            get_reg.return_value = registry
            _sync_registry_from_db(db)

        registry.sync_from_catalog.assert_called_once_with(catalog)
        conn.close.assert_called_once()

    def test_handles_sync_exception(self):
        from plone.pgcatalog.config import _sync_registry_from_db

        db = mock.Mock()
        conn = mock.Mock()
        db.open.return_value = conn

        site = mock.Mock()
        catalog = mock.Mock()
        catalog._catalog = mock.Mock()
        site.portal_catalog = catalog

        root = mock.Mock()
        root.get.return_value = root
        root.values.return_value = [site]
        conn.root.return_value = root

        with mock.patch("plone.pgcatalog.columns.get_registry") as get_reg:
            registry = mock.Mock()
            registry.sync_from_catalog.side_effect = RuntimeError("sync failed")
            get_reg.return_value = registry
            # Should not raise
            _sync_registry_from_db(db)

        conn.close.assert_called_once()

    def test_handles_root_traversal_exception(self):
        from plone.pgcatalog.config import _sync_registry_from_db

        db = mock.Mock()
        conn = mock.Mock()
        db.open.return_value = conn
        conn.root.side_effect = RuntimeError("db error")

        with mock.patch("plone.pgcatalog.columns.get_registry"):
            # Should not raise
            _sync_registry_from_db(db)

        conn.close.assert_called_once()


class TestOrjsonLoader:
    """Phase 1: orjson JSONB loader."""

    def test_orjson_is_installed(self):
        import orjson

        # orjson.loads returns bytes → same Python types as json.loads
        result = orjson.loads(b'{"key": "value"}')
        assert result == {"key": "value"}
