from typing import Optional, List, Dict, Any
from .data_access_util import (
    TransactionalDataAccess,
    handle_database_operation,
)
import datetime


class LabDataAccess(TransactionalDataAccess):
    """
    Concrete Data Access class for the 'lab_results' table.
    Inherits from TransactionalDataAccess to simplify common operations.
    """

    def __init__(self, db_path: str):
        """
        Initializes the LabDataAccess object.
        Calls the TransactionalDataAccess constructor
        with the table-specific information.
        """
        table_name = "lab_results"
        column_definitions = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "patient_id INTEGER",
            "logtime BIGINT NOT NULL",  # Timestamp where data got added to the table
            "sampled_time BIGINT NOT NULL",  # Datetime when blood sample is being taken
            "result_time BIGINT NOT NULL",  # Datetime when lab result is available
            "dtx INT",  # Dextrostix (mg/dL) measurement value by fingertip
            "ph DOUBLE",  # pH level of the blood
            "k DOUBLE",  # Potassium level in the blood (mmol/L || mEq/L)
            "na DOUBLE",  # Sodium level in the blood (mmol/L || mEq/L)
            "FOREIGN KEY (patient_id) REFERENCES patient(patient_id) ON UPDATE CASCADE ON DELETE CASCADE",
        ]
        super().__init__(db_path, table_name, column_definitions)
        self.id_column_name = "id"

    def insert(self, data: Dict[str, Any]) -> int:
        """Inserts a new lab result record."""
        required_keys = {"patient_id", "logtime", "sampled_time", "result_time"}
        if not required_keys.issubset(data.keys()):
            missing = required_keys - data.keys()
            raise ValueError(f"Missing required keys for insert: {missing}")

        # Validate 'dtx' value
        if "dtx" in data and data["dtx"] is not None and data["dtx"] < 0:
            raise ValueError("'dtx' value cannot be negative.")

        insert_data = {
            "patient_id": data["patient_id"],
            "logtime": data["logtime"],
            "sampled_time": data["sampled_time"],
            "result_time": data["result_time"],
            "dtx": data.get("dtx"),  # Use .get() with None default for optional fields
            "ph": data.get("ph"),
            "k": data.get("k"),
            "na": data.get("na"),
        }
        return super().insert(insert_data)

    def get_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a lab result by its primary key ID."""
        return super().get_by_id(record_id)

    def update(self, record_id: int, data: Dict[str, Any]) -> bool:
        """Updates an existing lab result record."""
        if not data:
            raise ValueError("No data provided for update.")

        update_data = {
            "logtime": data.get("logtime"),
            "sampled_time": data.get("sampled_time"),
            "result_time": data.get("result_time"),
            "dtx": data.get("dtx"),
            "ph": data.get("ph"),
            "k": data.get("k"),
            "na": data.get("na"),
        }
        update_data = {k: v for k, v in update_data.items() if v is not None}
        return super().update(record_id, update_data)

    def delete(self, record_id: int) -> bool:
        """Deletes a lab result record by its ID."""
        return super().delete(record_id)

    def get_all(self) -> List[Dict[str, Any]]:
        """Retrieves all lab result records."""
        return super().get_all()


if __name__ == "__main__":
    db_path = "./src/data/dka_data.db"  # Use a distinct name for testing
    # Instantiate the specific DAO implementation
    lab_dao = LabDataAccess(db_path)

    lab_id_to_use: Optional[Optional[int]] = None  # Variable to store the ID

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
            "sampled_time": datetime.datetime.fromisoformat(
                SAMPLED_TIME_MOCK
            ).timestamp()
            * 1000,
            "result_time": datetime.datetime.fromisoformat(RESULT_TIME_MOCK).timestamp()
            * 1000,
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
                print(f"   Lab result update status: {was_updated}")
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
                print(f"   Lab result delete status: {was_deleted}")
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
