"""internal_etl_package — Maria's table-replication library.

Reliability logic (merger, quality, ledger, alerting, dag_factory) lives in
this package and talks to compute engines only through the TableEngine
interface in internal_etl_package.engines.base. Engine-library imports are
confined to internal_etl_package/engines/ — that rule is enforced by
test_architecture.py.
"""


class TableNotFound(Exception):
    """Raised when a table is referenced that does not exist in the catalog."""


class DataQualityError(Exception):
    """Raised when a quality check fails hard enough to block downstream work."""
