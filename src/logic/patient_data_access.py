import sqlite3
from typing import Optional, List, Dict, Any
from data_access_util import (
    DataAccess,
    DatabaseError,
    QueryError,
    RecordNotFoundError,
    handle_database_operation,  # Needed for example usage
)


class PatientDataAccess(DataAccess):
    """
    Data access class for managing patient data ('patients' table).

    Inherits from DataAccess and implements database operations for patients,
    managing its own connection and transactions for individual operations.
    """

    def __init__(self, db_path: str):
        """
        Initializes the PatientDataAccess object.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        super().__init__(db_path)
        self.table_name = "patients"

    def create_table(self) -> None:
        """Creates the patients table if it doesn't exist."""
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                patient_id INTEGER PRIMARY KEY AUTOINCREMENT,
                hn TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                sex TEXT NOT NULL,
                medical_history TEXT
            )
        """
        # _execute_query handles connection and basic errors
        self._execute_query(query)
        # Commit DDL changes explicitly for robustness
        if self._connection:
            try:
                self._connection.commit()
            except sqlite3.Error as e:
                raise QueryError(
                    f"Failed to commit after creating table '{self.table_name}': {e}"
                ) from e

    def insert(self, data: Dict[str, Any]) -> int:
        """
        Inserts a new patient record into the patients table.

        Args:
            data (dict): A dictionary containing patient data (hn, name, age, sex, medical_history).

        Returns:
            int: The ID of the newly inserted patient record (patient_id).

        Raises:
            ValueError: If required keys are missing in data.
            QueryError: If the database insertion query fails.
            DatabaseError: For other database-related errors during insert.
        """
        required_keys = {"hn", "name", "age", "sex"}  # medical_history is optional
        if not required_keys.issubset(data.keys()):
            missing = required_keys - data.keys()
            raise ValueError(f"Missing required keys for patient insert: {missing}")

        # Use named placeholders for clarity with dictionary data
        query = f"""
            INSERT INTO {self.table_name} (hn, name, age, sex, medical_history)
            VALUES (:hn, :name, :age, :sex, :medical_history)
        """
        # Ensure optional fields exist in the dictionary or provide defaults
        params = {
            "hn": data["hn"],
            "name": data["name"],
            "age": data["age"],
            "sex": data["sex"],
            "medical_history": data.get(
                "medical_history", None
            ),  # Use .get for optional field
        }

        try:
            cursor = self._execute_query(query, params)
            if cursor.lastrowid is None:
                # This case might indicate an issue even if no exception was raised
                raise DatabaseError("Patient insertion failed, lastrowid is None")

            # Commit the change explicitly
            if self._connection:
                self._connection.commit()
            return cursor.lastrowid
        except (QueryError, DatabaseError) as e:
            if self._connection:
                self._connection.rollback()  # Rollback on error
            raise e  # Re-raise the original DB error
        except Exception as e:  # Catch unexpected errors
            if self._connection:
                self._connection.rollback()
            # Wrap unexpected errors for consistency
            raise DatabaseError(f"Unexpected error during patient insert: {e}") from e

    def get_by_id(self, patient_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves a patient record by their ID.

        Args:
            patient_id (int): The ID of the patient (patient_id) to retrieve.

        Returns:
            Optional[dict]: A dictionary containing the patient's data, or None if not found.

        Raises:
            QueryError: If the database query fails.
        """
        query = f"""
            SELECT patient_id, hn, name, age, sex, medical_history
            FROM {self.table_name}
            WHERE patient_id = ?
        """
        cursor = self._execute_query(query, (patient_id,))
        row = cursor.fetchone()
        # Convert sqlite3.Row to dict if found, otherwise return None
        return dict(row) if row else None

    def update(self, patient_id: int, data: Dict[str, Any]) -> bool:
        """
        Updates an existing patient record.

        Args:
            patient_id (int): The ID of the patient (patient_id) to update.
            data (dict): Dictionary containing the fields and values to update.

        Returns:
            bool: True if a row was updated, False otherwise.

        Raises:
            ValueError: If the data dictionary is empty.
            RecordNotFoundError: If no patient with the given ID (patient_id) exists.
            QueryError: If the database update query fails.
            DatabaseError: For other database-related errors during update.
        """
        if not data:
            raise ValueError("No data provided for patient update.")

        # Check if record exists before attempting update for better error context
        if self.get_by_id(patient_id) is None:
            raise RecordNotFoundError(
                f"Cannot update patient. Record with ID (patient_id) {patient_id} not found."
            )

        # Dynamically build the SET part of the query
        fields = ", ".join([f"{key} = :{key}" for key in data.keys()])
        query = f"UPDATE {self.table_name} SET {fields} WHERE patient_id = :patient_id"

        # Combine update data with the record ID for parameter binding
        update_params = data.copy()
        update_params["patient_id"] = patient_id

        try:
            cursor = self._execute_query(query, update_params)
            updated = cursor.rowcount > 0  # Check if any rows were affected

            # Commit the change explicitly
            if self._connection:
                self._connection.commit()
            return updated
        except (QueryError, DatabaseError) as e:
            if self._connection:
                self._connection.rollback()
            raise e
        except Exception as e:
            if self._connection:
                self._connection.rollback()
            raise DatabaseError(f"Unexpected error during patient update: {e}") from e

    def delete(self, patient_id: int) -> bool:
        """
        Deletes a patient record by their ID.

        Args:
            patient_id (int): The ID of the patient to delete.

        Returns:
            bool: True if a row was deleted, False otherwise.

        Raises:
            QueryError: If the database deletion query fails.
            DatabaseError: For other database-related errors during delete.
        """
        # Optional: Add a check using get_by_id first if you want to raise RecordNotFoundError
        # if self.get_by_id(patient_id) is None:
        #     raise RecordNotFoundError(f"Cannot delete patient. Record with ID {patient_id} not found.")

        query = f"DELETE FROM {self.table_name} WHERE patient_id = ?"
        try:
            cursor = self._execute_query(query, (patient_id,))
            deleted = cursor.rowcount > 0  # Check if any rows were affected

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
            raise DatabaseError(f"Unexpected error during patient delete: {e}") from e

    def get_all(self) -> List[Dict[str, Any]]:
        """
        Retrieves all patient records from the patients table.

        Returns:
            List[dict]: A list of dictionaries representing patient records. Empty list if none found.

        Raises:
            QueryError: If the database query fails.
        """
        query = f"SELECT patient_id, hn, name, age, sex, medical_history FROM {self.table_name} ORDER BY name"
        cursor = self._execute_query(query)
        rows = cursor.fetchall()
        # Convert list of sqlite3.Row to list of dicts
        return [dict(row) for row in rows]


# =============================================================================
# Example Usage (Updated)
# =============================================================================
if __name__ == "__main__":
    # Define database path (adjust as needed)
    # Using a relative path for simplicity in example
    db_path = "./src/data/dka_data.db"  # Use a distinct name for testing
    patient_dao = PatientDataAccess(db_path)

    patient_id_to_use: Optional[int] = None  # Variable to store the ID

    print(f"--- Patient Database Operations on '{db_path}' ---")

    try:
        # DAO manages its own connection internally. No external 'with' needed for individual ops.

        # 1. Create table
        print("\n1. Attempting to create patient table...")
        result = handle_database_operation(lambda: patient_dao.create_table())
        if result.is_err():
            print(f"   Error creating table: {result.error}")
        else:
            print("   Patient table ensured successfully.")

        # 2. Insert a patient
        print("\n2. Attempting to insert patient...")
        patient_data = {
            "hn": "HN98765",  # Example HN
            "name": "Jane Smith",
            "age": 45,
            "sex": "Female",
            "medical_history": "Hypertension",
        }
        result = handle_database_operation(lambda: patient_dao.insert(patient_data))
        if result.is_ok():
            patient_id_to_use = result.unwrap()
            print(f"   Patient inserted successfully with ID: {patient_id_to_use}")
        else:
            print(f"   Error inserting patient: {result.error}")

        # --- Operations dependent on successful insert ---
        if patient_id_to_use is not None:
            # 3. Get patient by ID
            print(f"\n3. Attempting to get patient ID: {patient_id_to_use}...")
            result = handle_database_operation(
                lambda: patient_dao.get_by_id(patient_id_to_use)
            )
            if result.is_ok():
                patient = result.unwrap()
                if patient:
                    print(f"   Retrieved patient: {patient}")
                else:
                    # Should not happen if insert succeeded and ID is correct
                    print(
                        f"   Patient with ID {patient_id_to_use} not found (unexpected)."
                    )
            else:
                print(f"   Error getting patient: {result.error}")

            # 4. Update the patient
            print(f"\n4. Attempting to update patient ID: {patient_id_to_use}...")
            updated_data = {
                "name": "Jane Smith-Jones",  # Changed name
                "age": 46,  # Changed age
                "medical_history": "Hypertension, controlled",  # Updated history
            }
            # Note: We don't update 'hn' or 'sex' in this example update
            result = handle_database_operation(
                lambda: patient_dao.update(patient_id_to_use, updated_data)
            )
            if result.is_ok():
                was_updated = result.unwrap()
                print(f"   Patient update status (was row affected?): {was_updated}")
                # Verify update
                verify_result = handle_database_operation(
                    lambda: patient_dao.get_by_id(patient_id_to_use)
                )
                if verify_result.is_ok():
                    print(f"   Verified updated patient: {verify_result.unwrap()}")
                else:
                    print(f"   Error verifying update: {verify_result.error}")
            else:
                # Could be RecordNotFoundError if ID was wrong, or QueryError
                print(f"   Error updating patient: {result.error}")

            # 5. Get all patients (Placed before delete)
            print("\n5. Attempting to get all patients...")
            result = handle_database_operation(lambda: patient_dao.get_all())
            if result.is_ok():
                all_patients = result.unwrap()
                print(f"   All patients ({len(all_patients)} found):")
                for p in all_patients:
                    print(f"     - {p}")
            else:
                print(f"   Error getting all patients: {result.error}")

            # 6. Delete the patient
            print(f"\n6. Attempting to delete patient ID: {patient_id_to_use}...")
            result = handle_database_operation(
                lambda: patient_dao.delete(patient_id_to_use)
            )
            if result.is_ok():
                was_deleted = result.unwrap()
                print(f"   Patient delete status (was row affected?): {was_deleted}")
            else:
                print(f"   Error deleting patient: {result.error}")

            # 7. Get deleted patient (Should return Ok(None))
            print(f"\n7. Attempting to get deleted patient ID: {patient_id_to_use}...")
            result = handle_database_operation(
                lambda: patient_dao.get_by_id(patient_id_to_use)
            )
            if result.is_ok():
                deleted_patient = result.unwrap()
                if deleted_patient is None:
                    print(
                        "   Successfully confirmed patient is deleted (get_by_id returned None)."
                    )
                else:
                    # This shouldn't happen if delete was successful
                    print(f"   WARNING: Deleted patient still found: {deleted_patient}")
            else:
                # This would indicate an error during the SELECT query itself
                print(f"   Error trying to get deleted patient: {result.error}")

        else:
            print(
                "\nSkipping get/update/delete operations because insert failed or returned no ID."
            )

        # Example: Insert another patient to test get_all after delete
        print("\n8. Inserting another patient...")
        another_patient_data = {
            "hn": "HN11223",
            "name": "Peter Jones",
            "age": 62,
            "sex": "Male",
            "medical_history": "None",
        }
        result_ins2 = handle_database_operation(
            lambda: patient_dao.insert(another_patient_data)
        )
        if result_ins2.is_ok():
            print(f"   Inserted second patient with ID: {result_ins2.unwrap()}")
            # Get all again to see the remaining patient
            result_all2 = handle_database_operation(lambda: patient_dao.get_all())
            if result_all2.is_ok():
                print(f"   All patients after second insert: {result_all2.unwrap()}")
            else:
                print(
                    f"   Error getting all patients after second insert: {result_all2.error}"
                )
        else:
            print(f"   Error inserting second patient: {result_ins2.error}")

    except Exception as e:
        # Catch any unexpected errors outside the handle_database_operation wrapper
        print("\n--- UNEXPECTED SCRIPT ERROR ---")
        print(f"An error occurred: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # IMPORTANT: Ensure the DAO connection is closed when the script is done
        print("\n--- Cleaning up ---")
        if patient_dao.check_connection():
            print("Closing DAO database connection...")
            patient_dao._close_connection()
        else:
            print("DAO connection already closed or never opened.")

    print("\n--- Script finished ---")
