from typing import Optional, List, Dict, Any
from data_access_util import (
    TransactionalDataAccess,
    handle_database_operation,
)


class PatientDataAccess(TransactionalDataAccess):
    """
    Data access class for managing patient data ('patients' table).
    Inherits from TransactionalDataAccess to simplify common operations.
    """

    def __init__(self, db_path: str):
        """
        Initializes the PatientDataAccess object.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        table_name = "patients"
        column_definitions = [
            "patient_id INTEGER PRIMARY KEY AUTOINCREMENT",
            "hn TEXT NOT NULL UNIQUE",
            "name TEXT NOT NULL",
            "age INTEGER NOT NULL",
            "sex TEXT NOT NULL",
            "medical_history TEXT",
        ]
        super().__init__(db_path, table_name, column_definitions)
        self.id_column_name = "patient_id"  # Correctly set the id_column_name

    def insert(self, data: Dict[str, Any]) -> int:
        """Inserts a new patient record."""
        required_keys = {"hn", "name", "age", "sex"}
        if not required_keys.issubset(data.keys()):
            missing = required_keys - data.keys()
            raise ValueError(f"Missing keys: {missing}")
        insert_data = {
            "hn": data["hn"],
            "name": data["name"],
            "age": data["age"],
            "sex": data["sex"],
            "medical_history": data.get("medical_history"),
        }
        return super().insert(insert_data)

    def get_by_id(self, patient_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a patient record by ID."""
        return super().get_by_id(patient_id)

    def update(self, patient_id: int, data: Dict[str, Any]) -> bool:
        """Updates an existing patient record."""
        if not data:
            raise ValueError("No update data provided.")
        update_data = {
            "name": data.get("name"),
            "age": data.get("age"),
            "sex": data.get("sex"),
            "medical_history": data.get("medical_history"),
        }
        update_data = {k: v for k, v in update_data.items() if v is not None}
        return super().update(patient_id, update_data)

    def delete(self, patient_id: int) -> bool:
        """Deletes a patient record by ID."""
        return super().delete(patient_id)

    def get_all(self) -> List[Dict[str, Any]]:
        """Retrieves all patient records."""
        return super().get_all()


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
                    print(f"   WARNING: Deleted patient still found: {deleted_patient}")
            else:
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
