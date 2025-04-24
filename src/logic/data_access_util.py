import sqlite3
from abc import ABC, abstractmethod
from typing import Optional, List, Any, Callable, Dict, Union

# =============================================================================
# 1. Custom Exception Hierarchy
# =============================================================================
class DatabaseError(Exception):
    """Base class for custom database-related exceptions."""

    pass


class QueryError(DatabaseError):
    """Exception raised when a database query fails."""

    pass


class RecordNotFoundError(DatabaseError):
    """Exception raised when a record is not found, potentially during update/delete."""

    pass


# =============================================================================
# 2. Database Connection Management (Context Manager for Transactions)
# =============================================================================


# Note: This context manager is useful if you need to wrap MULTIPLE DAO
# operations within a single transaction. However, the DataAccess class below
# manages its own connection for individual operations by default.
# Using this context manager requires modifying the DAO to accept an optional
# external connection. For simplicity, the example usage below relies on the
# DAO's internal connection management.
class DatabaseConnection:
    """
    Manages SQLite database connections using a context manager.

    Primarily useful for ensuring atomic transactions across multiple operations
    if the DAO is adapted to use an externally provided connection.
    Ensures connections are properly opened and closed, even if errors occur.
    """

    def __init__(self, db_path: str):
        """
        Initializes the DatabaseConnection.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        self.db_path = db_path
        self.connection: Optional[sqlite3.Connection] = None

    def __enter__(self) -> sqlite3.Connection:
        """
        Opens a database connection when entering the `with` block.

        Returns:
            sqlite3.Connection: The database connection object.
        """
        try:
            # Add type detection and row factory for convenience
            self.connection = sqlite3.connect(
                self.db_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
            self.connection.row_factory = sqlite3.Row
            return self.connection
        except sqlite3.Error as e:
            self.connection = None  # Ensure connection is None if connect fails
            raise DatabaseError(
                f"Error connecting to the database '{self.db_path}': {e}"
            ) from e

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[Exception],
        traceback: Optional[Any],
    ) -> None:
        """
        Closes the database connection when exiting the `with` block.

        Handles commit/rollback based on whether an exception occurred.
        """
        if self.connection:
            try:
                if exc_type is None:
                    # No exception in the 'with' block, try to commit
                    self.connection.commit()
                else:
                    # An exception occurred, rollback
                    self.connection.rollback()
            except sqlite3.Error as e:
                # Handle potential errors during commit/rollback itself
                print(f"CRITICAL WARNING: Commit/Rollback failed: {e}")
            finally:
                # Ensure close is always attempted
                self.connection.close()
                self.connection = None  # Clear the reference


# =============================================================================
# 3. Abstract Data Access Class (Manages its own connection)
# =============================================================================


class DataAccess(ABC):
    """
    Abstract base class for data access objects.

    Manages its own database connection internally for operations.
    Subclasses implement specific table interactions.
    """

    def __init__(self, db_path: str):
        """
        Initializes the DataAccess object with the database path.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        self.db_path = db_path
        # Connection managed internally by the instance
        self._connection: Optional[sqlite3.Connection] = None

    def _get_connection(self) -> sqlite3.Connection:
        """
        Gets the internal database connection, creating it if it doesn't exist.
        Uses type detection and dictionary row factory.
        """
        if self._connection is None:
            try:
                self._connection = sqlite3.connect(
                    self.db_path,
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                )
                # Use sqlite3.Row for attribute access and dict conversion
                self._connection.row_factory = sqlite3.Row
            except sqlite3.Error as e:
                # Ensure connection remains None if connection fails
                self._connection = None
                raise DatabaseError(
                    f"Error connecting to the database '{self.db_path}': {e}"
                ) from e
        return self._connection

    def _close_connection(self) -> None:
        """Closes the internal database connection if it's open."""
        if self._connection:
            try:
                self._connection.close()
            except sqlite3.Error as e:
                # Log error during close, but don't typically raise
                print(f"Warning: Error closing connection: {e}")
            finally:
                self._connection = None  # Clear reference regardless

    def check_connection(self) -> bool:
        """Checks if an internal connection is currently open."""
        return self._connection is not None

    def _execute_query(
        self, query: str, parameters: Union[tuple, Dict[str, Any]] = ()
    ) -> sqlite3.Cursor:
        """
        Executes a database query using the internal connection with error handling.

        Args:
            query (str): The SQL query to execute.
            parameters (Union[tuple, Dict[str, Any]], optional): Parameters for the query.
                Defaults to ().

        Returns:
            sqlite3.Cursor: The cursor object from the executed query.

        Raises:
            QueryError: If the query fails to execute.
            DatabaseError: If connection fails.
        """
        conn = self._get_connection()  # Get or create internal connection
        try:
            cursor = conn.cursor()
            cursor.execute(query, parameters)
            # NOTE: For operations modifying data (INSERT, UPDATE, DELETE),
            # SQLite in autocommit mode (default outside explicit transaction)
            # commits each statement. If transactional behavior is needed
            # across multiple _execute_query calls, an explicit transaction
            # must be started (e.g., conn.execute('BEGIN TRANSACTION')) or
            # use the DatabaseConnection context manager externally.
            return cursor
        except sqlite3.Error as e:
            raise QueryError(f"Error executing query: {query} - {e}") from e

    # --- Abstract methods to be implemented by subclasses ---

    @abstractmethod
    def create_table(self) -> None:
        """Abstract method to create the specific table."""
        pass

    @abstractmethod
    def insert(self, data: Dict[str, Any]) -> int:
        """Abstract method to insert data. Returns new row ID."""
        pass

    @abstractmethod
    def get_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Abstract method to retrieve data by ID."""
        pass

    @abstractmethod
    def update(self, record_id: int, data: Dict[str, Any]) -> bool:
        """Abstract method to update data. Returns True if updated."""
        pass

    @abstractmethod
    def delete(self, record_id: int) -> bool:
        """Abstract method to delete data. Returns True if deleted."""
        pass

    @abstractmethod
    def get_all(self) -> List[Dict[str, Any]]:
        """Abstract method to get all records."""
        pass


# =============================================================================
# 5. Monad-Inspired Error Handling (Result Class)
# =============================================================================
class Result:
    """
    A simple class to represent the result of an operation that might fail.
    Inspired by the Result Monad concept.
    """

    # Using _is_ok internally to avoid potential conflicts if 'value' is a boolean
    def __init__(
        self,
        is_ok: bool,
        value: Optional[Any] = None,
        error: Optional[Exception] = None,
    ):
        self._is_ok = is_ok
        self.value = value
        self.error = error

    def is_ok(self) -> bool:
        """Returns True if the result is Ok (success), False otherwise."""
        return self._is_ok

    def is_err(self) -> bool:
        """Returns True if the result is Err (failure), False otherwise."""
        return not self._is_ok

    def unwrap(self) -> Any:
        """
        Returns the value if the result is Ok, raises the stored error if Err.
        """
        if self.is_ok():
            return self.value
        elif self.error:
            # Raise the specific error that was caught
            raise self.error
        else:
            # Should not happen with proper usage, but safeguard
            raise ValueError("Attempted to unwrap an Err result with no error stored.")

    @staticmethod
    def Ok(value: Any = None) -> "Result":  # Allow Ok(None)
        """Creates a Result object in the Ok state."""
        return Result(is_ok=True, value=value)

    @staticmethod
    def Err(error: Exception) -> "Result":
        """Creates a Result object in the Err state."""
        # Ensure an actual exception object is passed
        if not isinstance(error, Exception):
            error = TypeError(f"Non-exception type passed to Result.Err: {type(error)}")
        return Result(is_ok=False, error=error)


# =============================================================================
# 6. Error Handling Wrapper Function
# =============================================================================
def handle_database_operation(operation: Callable[[], Any]) -> Result:
    """
    Handles a database operation using the DAO and returns a Result object.

    Wraps a DAO method call (which manages its own connection and errors)
    and catches expected custom database exceptions, returning a Result object.

    Args:
        operation (Callable[[], Any]): A zero-argument function that calls a DAO method.

    Returns:
        Result: A Result object (Ok or Err) representing the outcome.
    """
    try:
        # Execute the DAO method (e.g., lab_dao.create_table())
        result_value = operation()
        return Result.Ok(result_value)
    except (
        DatabaseError,
        ValueError,
        TypeError,
    ) as e:  # Catch DatabaseError subclasses and data errors
        # Log the error for diagnostics if needed
        # print(f"Database operation failed: {type(e).__name__}: {e}")
        return Result.Err(e)
    except Exception as e:
        # Catch unexpected errors during the operation
        print(
            f"An unexpected error occurred during DB operation: {type(e).__name__}: {e}"
        )
        # Log traceback here if desired
        return Result.Err(e)