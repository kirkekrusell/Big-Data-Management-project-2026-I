from abc import ABC, abstractmethod


class TableEngine(ABC):
    """The seam between reliability logic and compute engines.

    Implementations live in this package. Code outside `engines/` MUST NOT
    import any specific engine library — it talks to the abstract base class.
    """

    @abstractmethod
    def current_snapshot_id(self, table: str) -> int:
        """Return the snapshot id of the latest committed version of `table`."""

    @abstractmethod
    def rollback_to_snapshot(self, table: str, snapshot_id: int) -> None:
        """Restore `table` to the given historical snapshot."""

    @abstractmethod
    def merge(self, table: str, source_path: str, primary_key: str) -> int:
        """Apply pending source files to `table`. Return the new snapshot id."""

    @abstractmethod
    def count_duplicates(self, table: str, primary_key: str) -> int:
        """Return how many `primary_key` values appear more than once."""

    @abstractmethod
    def row_count(self, table: str) -> int:
        """Return the total number of rows in `table`."""

    @abstractmethod
    def null_rate(self, table: str, column: str) -> float:
        """Return the fraction of rows where `column` is NULL (0.0 to 1.0)."""
