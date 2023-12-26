# dialects/oracle/oracledb.py
# Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

r"""
.. dialect:: oracle+oracledb
    :name: python-oracledb
    :dbapi: oracledb
    :connectstring: oracle+oracledb://user:pass@hostname:port[/dbname][?service_name=<service>[&key=value&key=value...]]
    :url: https://oracle.github.io/python-oracledb/

python-oracledb is released by Oracle to supersede the cx_Oracle driver.
It is fully compatible with cx_Oracle and features both a "thin" client
mode that requires no dependencies, as well as a "thick" mode that uses
the Oracle Client Interface in the same way as cx_Oracle.

.. seealso::

    :ref:`cx_oracle` - all of cx_Oracle's notes apply to the oracledb driver
    as well.

Thick mode support
------------------

By default the ``python-oracledb`` is started in thin mode, that does not
require oracle client libraries to be installed in the system. The
``python-oracledb`` driver also support a "thick" mode, that behaves
similarly to ``cx_oracle`` and requires that Oracle Client Interface (OCI)
is installed.

To enable this mode, the user may call ``oracledb.init_oracle_client``
manually, or by passing the parameter ``thick_mode=True`` to
:func:`_sa.create_engine`. To pass custom arguments to ``init_oracle_client``,
like the ``lib_dir`` path, a dict may be passed to this parameter, as in::

    engine = sa.create_engine("oracle+oracledb://...", thick_mode={
        "lib_dir": "/path/to/oracle/client/lib", "driver_name": "my-app"
    })

.. seealso::

    https://python-oracledb.readthedocs.io/en/latest/api_manual/module.html#oracledb.init_oracle_client


.. versionadded:: 2.0.0 added support for oracledb driver.

"""  # noqa
import re

from sqlalchemy import util
from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_connection
from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_cursor
from sqlalchemy.dialects.oracle.base import OracleDialect
from .cx_oracle import OracleDialect_cx_oracle as _OracleDialect_cx_oracle
from ... import exc
from ... import pool
from ...util.concurrency import await_fallback
from ...util.concurrency import await_only


class OracleDialect_oracledb(_OracleDialect_cx_oracle):
    supports_statement_cache = True
    driver = "oracledb"

    def __init__(
        self,
        auto_convert_lobs=True,
        coerce_to_decimal=True,
        arraysize=50,
        encoding_errors=None,
        thick_mode=None,
        **kwargs,
    ):
        super().__init__(
            auto_convert_lobs,
            coerce_to_decimal,
            arraysize,
            encoding_errors,
            **kwargs,
        )

        if self.dbapi is not None and (
            thick_mode or isinstance(thick_mode, dict)
        ):
            kw = thick_mode if isinstance(thick_mode, dict) else {}
            self.dbapi.init_oracle_client(**kw)

    @classmethod
    def import_dbapi(cls):
        import oracledb

        return oracledb

    @classmethod
    def get_async_dialect_cls(cls, url):
        return OracleDialectAsync_oracledb

    @classmethod
    def is_thin_mode(cls, connection):
        return connection.connection.dbapi_connection.thin

    def _load_version(self, dbapi_module):
        version = (0, 0, 0)
        if dbapi_module is not None:
            m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", dbapi_module.version)
            if m:
                version = tuple(
                    int(x) for x in m.group(1, 2, 3) if x is not None
                )
        self.oracledb_ver = version
        if self.oracledb_ver < (1,) and self.oracledb_ver > (0, 0, 0):
            raise exc.InvalidRequestError(
                "oracledb version 1 and above are supported"
            )


class AsyncAdapt_oracledb_cursor(AsyncAdapt_dbapi_cursor):
    __slots__ = ()

    @property
    def outputtypehandler(self):
        return self._cursor.outputtypehandler

    @outputtypehandler.setter
    def outputtypehandler(self, value):
        self._cursor.outputtypehandler = value

    def close(self):
        self._rows.clear()
        self._cursor.close()


class AsyncAdapt_oracledb_connection(AsyncAdapt_dbapi_connection):
    __slots__ = ()
    _cursor_cls = AsyncAdapt_oracledb_cursor

    @property
    def autocommit(self):
        return self._connection.autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        self._connection.autocommit = value

    @property
    def outputtypehandler(self):
        return self._connection.outputtypehandler

    @outputtypehandler.setter
    def outputtypehandler(self, value):
        self._connection.outputtypehandler = value

    @property
    def version(self):
        return self._connection.version

    def character_set_name(self):
        return self._connection.encoding

    def close(self):
        self.await_(self._connection.close())


class AsyncAdaptFallback_oracledb_connection(
    AsyncAdapt_oracledb_connection, AsyncAdapt_dbapi_connection
):
    __slots__ = ()


class AsyncAdapt_oracledb_dbapi:
    def __init__(self, oracledb) -> None:
        self.oracledb = oracledb
        self.paramstyle = OracleDialect.default_paramstyle
        for name in dir(oracledb):
            if not hasattr(self, name):
                setattr(self, name, getattr(oracledb, name))

    def connect(self, *arg, **kw):
        async_fallback = kw.pop("async_fallback", False)
        creator_fn = kw.pop("async_creator_fn", self.oracledb.connect_async)

        if util.asbool(async_fallback):
            return AsyncAdaptFallback_oracledb_connection(
                self,
                await_fallback(creator_fn(*arg, **kw)),
            )
        else:
            return AsyncAdapt_oracledb_connection(
                self,
                await_only(creator_fn(*arg, **kw)),
            )


class OracleDialectAsync_oracledb(OracleDialect_oracledb):
    is_async = True
    driver = "oracledb"

    def __init__(
        self,
        auto_convert_lobs=True,
        coerce_to_decimal=True,
        arraysize=50,
        encoding_errors=None,
        **kwargs,
    ):
        super().__init__(
            auto_convert_lobs,
            coerce_to_decimal,
            arraysize,
            encoding_errors,
            thick_mode=None,
            **kwargs,
        )

    @classmethod
    def import_dbapi(cls):
        oracledb = __import__("oracledb")
        # oracledb.connect = oracledb.connect_async
        # oracledb.Connection = oracledb.AsyncConnection
        # oracledb.ConnectionPool = oracledb.AsyncConnectionPool
        # oracledb.LOB = oracledb.AsyncLOB
        return AsyncAdapt_oracledb_dbapi(oracledb)

    @classmethod
    def is_thin_mode(cls, connection):
        # Only thin mode is supported by oracledb async
        return True

    @classmethod
    def get_pool_class(cls, url):
        async_fallback = url.query.get("async_fallback", False)

        if util.asbool(async_fallback):
            return pool.FallbackAsyncAdaptedQueuePool
        else:
            return pool.AsyncAdaptedQueuePool

    def _load_version(self, dbapi_module):
        return super()._load_version(dbapi_module.oracledb)


dialect = OracleDialect_oracledb
dialect_async = OracleDialectAsync_oracledb
