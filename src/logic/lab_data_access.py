import sqlite3
from typing import Optional, List, Dict, Any
from data_access_util import (
    DataAccess,
    DatabaseError,
    QueryError,
    RecordNotFoundError,
    handle_database_operation,
)
import datetime


class LabDataAccess(DataAccess):
    """
    Concrete Data Access class for the 'lab_results' table.
    Inherits from DataAccess and implements its abstract methods.
    """

    def create_table(self) -> None:
        """Creates the lab_results table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS lab_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id INTEGER,
            logtime BIGINT NOT NULL, -- Timestamp where data got added to the table
            sampled_time BIGINT NOT NULL, -- Datetime when blood sample is being taken
            result_time BIGINT NOT NULL, -- Datetime when lab result is available
            dtx INT, -- Dextrostix (mg/dL) measurement value by fingertip
            ph DOUBLE, -- pH level of the blood
            k DOUBLE, -- Potassium level in the blood (mmol/L || mEq/L)
            na DOUBLE, -- Sodium level in the blood (mmol/L || mEq/L)

            FOREIGN KEY (patient_id) REFERENCES patient(patient_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );
        """
        # _execute_query handles connection and basic errors
        self._execute_query(create_table_sql)
        # Need to commit DDL changes if not in autocommit mode, but CREATE TABLE IF NOT EXISTS is usually safe.
        # For robustness with potential future non-autocommit modes:
        if self._connection:
            try:
                self._connection.commit()
            except sqlite3.Error as e:
                raise QueryError(f"Failed to commit after creating table: {e}") from e

    def insert(self, data: Dict[str, Any]) -> int:
        """Inserts a new lab result record."""
        required_keys = {"logtime", "dtx", "ph", "k", "na"}
        if not required_keys.issubset(data.keys()):
            missing = required_keys - data.keys()
            # Use ValueError for bad input data before DB interaction
            raise ValueError(f"Missing required keys for insert: {missing}")

        sql = """
            INSERT INTO lab_results (logtime, sampled_time, result_time, dtx, ph, k, na)
            VALUES (:logtime, :sampled_time, :result_time, :dtx, :ph, :k, :na)
        """
        try:
            # Ensure times are appropriate objects before passing to DB
            # Example conversion (adapt as needed based on input format):
            # data['logtime'] = datetime.datetime.fromisoformat(data['logtime']) if isinstance(data.get('logtime'), str) else data.get('logtime')
            # ... similar for other timestamps ...

            cursor = self._execute_query(sql, data)
            if cursor.lastrowid is None:
                raise DatabaseError("Insertion failed, lastrowid is None")

            # Commit the change explicitly as it modifies data
            if self._connection:
                self._connection.commit()
            return cursor.lastrowid
        except (TypeError, ValueError) as e:
            # Catch potential time conversion errors or other data issues
            raise ValueError(f"Error processing data for insert: {e}") from e
        except (
            QueryError,
            DatabaseError,
        ) as e:  # Catch DB errors from _execute_query or commit
            if self._connection:
                self._connection.rollback()  # Rollback on error
            raise e  # Re-raise the original DB error
        except Exception as e:  # Catch unexpected errors
            if self._connection:
                self._connection.rollback()
            raise DatabaseError(f"Unexpected error during insert: {e}") from e

    def get_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a lab result by its primary key ID."""
        sql = "SELECT * FROM lab_results WHERE patient_id = ?"
        cursor = self._execute_query(sql, (record_id,))
        row = cursor.fetchone()
        return dict(row) if row else None  # Convert sqlite3.Row to dict

    def update(self, record_id: int, data: Dict[str, Any]) -> bool:
        """Updates an existing lab result record."""
        if not data:
            # Use ValueError for invalid input
            raise ValueError("No data provided for update.")

        # Basic check if record exists before attempting update
        if self.get_by_id(record_id) is None:
            raise RecordNotFoundError(
                f"Cannot update. Record with ID {record_id} not found."
            )

        fields = ", ".join([f"{key} = :{key}" for key in data.keys()])
        sql = f"UPDATE lab_results SET {fields} WHERE id = :id"

        update_data = data.copy()
        update_data["id"] = record_id

        try:
            # Add time conversions if necessary
            cursor = self._execute_query(sql, update_data)
            updated = cursor.rowcount > 0

            # Commit the change explicitly
            if self._connection:
                self._connection.commit()
            return updated
        except (TypeError, ValueError) as e:
            raise ValueError(f"Error processing data for update: {e}") from e
        except (QueryError, DatabaseError) as e:
            if self._connection:
                self._connection.rollback()
            raise e
        except Exception as e:
            if self._connection:
                self._connection.rollback()
            raise DatabaseError(f"Unexpected error during update: {e}") from e

    def delete(self, record_id: int) -> bool:
        """Deletes a lab result record by its ID."""
        # Optional: Check if record exists before attempting delete
        # if self.get_by_id(record_id) is None:
        #     raise RecordNotFoundError(f"Cannot delete. Record with ID {record_id} not found.")

        sql = "DELETE FROM lab_results WHERE id = ?"
        try:
            cursor = self._execute_query(sql, (record_id,))
            deleted = cursor.rowcount > 0

            # Commit the change explicitly
            if self._connection:
                self._connection.commit()
            return deleted
        except (QueryError, DatabaseError) as e:
            if self._connection:
                self._connection.rollback()
            raise e
        except Exception as e:
            if self._connection:
                self._connection.rollback()
            raise DatabaseError(f"Unexpected error during delete: {e}") from e

    def get_all(self) -> List[Dict[str, Any]]:
        """Retrieves all lab result records."""
        sql = "SELECT * FROM lab_results ORDER BY logtime"
        cursor = self._execute_query(sql)
        rows = cursor.fetchall()
        return [
            dict(row) for row in rows
        ]  # Convert list of sqlite3.Row to list of dicts


if __name__ == "__main__":
    db_path = "./src/data/dka_data.db"  # Use a distinct name for testing
    # Instantiate the specific DAO implementation
    lab_dao = LabDataAccess(db_path)

    lab_id_to_use: Optional[int] = None  # Variable to store the ID

    print(f"--- Database Operations on '{db_path}' ---")

    try:
        # No outer 'with DatabaseConnection' needed here, as DAO manages connections internally.
        # Each operation uses the DAO's internal connection management.

        # 1. Create table
        print("\n1. Attempting to create table...")
        # Use lambda to pass a zero-argument callable to the handler
        result = handle_database_operation(lambda: lab_dao.create_table())
        if result.is_err():
            print(f"   Error creating table: {result.error}")
        else:
            print("   Table ensured successfully.")

        # 2. Insert lab result
        print("\n2. Attempting to insert lab result...")

        LOGTIME_MOCK = "2024-07-24T10:30:00.123+07:00"
        SAMPLED_TIME_MOCK = "2024-07-24T09:30:00.000+07:00"
        RESULT_TIME_MOCK = "2024-07-24T10:00:00.000+07:00"

        lab_data = {
            "patient_id": 1,
            "logtime": datetime.datetime.fromisoformat(LOGTIME_MOCK).timestamp() * 1000,
            "sampled_time": datetime.datetime.fromisoformat(SAMPLED_TIME_MOCK).timestamp() * 1000,
            "result_time": datetime.datetime.fromisoformat(RESULT_TIME_MOCK).timestamp() * 1000,
            "dtx": 120,
            "ph": 7.35,
            "k": 4.2,
            "na": 138,
        }
        result = handle_database_operation(lambda: lab_dao.insert(lab_data))
        if result.is_ok():
            lab_id_to_use = result.unwrap()
            print(f"   Lab result inserted successfully with ID: {lab_id_to_use}")
        else:
            print(f"   Error inserting lab result: {result.error}")

        # --- Operations dependent on successful insert ---
        if lab_id_to_use is not None:
            # 3. Get lab result by ID
            print(f"\n3. Attempting to get lab result ID: {lab_id_to_use}...")
            result = handle_database_operation(lambda: lab_dao.get_by_id(lab_id_to_use))
            if result.is_ok():
                lab_result = result.unwrap()
                if lab_result:
                    print(f"   Retrieved lab result: {lab_result}")
                else:
                    print(
                        f"   Lab result with ID {lab_id_to_use} not found (but operation was ok)."
                    )
            else:
                print(f"   Error getting lab result: {result.error}")

            # 4. Update lab result
            print(f"\n4. Attempting to update lab result ID: {lab_id_to_use}...")
            updated_data = {
                "logtime": datetime.datetime(2024, 7, 24, 11, 0, 0),
                "ph": 7.40,
                "k": 4.5,
            }
            result = handle_database_operation(
                lambda: lab_dao.update(lab_id_to_use, updated_data)
            )
            if result.is_ok():
                was_updated = result.unwrap()
                print(f"   Lab result update status (was row affected?): {was_updated}")
                # Verify update
                verify_result = handle_database_operation(
                    lambda: lab_dao.get_by_id(lab_id_to_use)
                )
                if verify_result.is_ok():
                    print(f"   Verified updated result: {verify_result.unwrap()}")
                else:
                    print(f"   Error verifying update: {verify_result.error}")
            else:
                print(
                    f"   Error updating lab result: {result.error}"
                )  # Could be RecordNotFoundError

            # 5. Get all lab results (Placed before delete)
            print("\n5. Attempting to get all lab results...")
            result = handle_database_operation(lambda: lab_dao.get_all())
            if result.is_ok():
                all_lab_results = result.unwrap()
                print(f"   All lab results ({len(all_lab_results)} found):")
                for res in all_lab_results:
                    print(f"     - {res}")
            else:
                print(f"   Error getting all lab results: {result.error}")

            # 6. Delete lab result
            print(f"\n6. Attempting to delete lab result ID: {lab_id_to_use}...")
            result = handle_database_operation(lambda: lab_dao.delete(lab_id_to_use))
            if result.is_ok():
                was_deleted = result.unwrap()
                print(f"   Lab result delete status (was row affected?): {was_deleted}")
            else:
                print(f"   Error deleting lab result: {result.error}")

            # 7. Get deleted lab result (Should return Ok(None))
            print(f"\n7. Attempting to get deleted lab result ID: {lab_id_to_use}...")
            result = handle_database_operation(lambda: lab_dao.get_by_id(lab_id_to_use))
            if result.is_ok():
                deleted_lab_result = result.unwrap()
                if deleted_lab_result is None:
                    print(
                        "   Successfully confirmed lab result is deleted (get_by_id returned None)."
                    )
                else:
                    # This shouldn't happen if delete was successful
                    print(
                        f"   WARNING: Deleted lab result still found: {deleted_lab_result}"
                    )
            else:
                # This would indicate an error during the SELECT query itself
                print(f"   Error trying to get deleted lab result: {result.error}")

        else:
            print(
                "\nSkipping get/update/delete operations because insert failed or returned no ID."
            )

    except Exception as e:
        # Catch any unexpected errors outside the handle_database_operation wrapper
        print("\n--- UNEXPECTED SCRIPT ERROR ---")
        print(f"An error occurred: {type(e).__name__}: {e}")
        # Log traceback here if needed
        import traceback

        traceback.print_exc()

    finally:
        # IMPORTANT: Ensure the DAO connection is closed when done
        print("\n--- Cleaning up ---")
        if lab_dao.check_connection():
            print("Closing DAO database connection...")
            lab_dao._close_connection()
        else:
            print("DAO connection already closed or never opened.")

    print("\n--- Script finished ---")
