import sqlite3
import os
import datetime
import traceback

# Import your data access classes
from data_access_util import (
    DatabaseConnection,
    handle_database_operation,
)
from lab_data_access import LabDataAccess
from patient_data_access import PatientDataAccess
from treatment_data_access import (
    DimTreatmentDataAccess,
    DimAdministrationTypeDataAccess,
    TreatmentDataAccess,
)

# Set up test database path
TEST_DB_PATH = "./src/data/test_dka_data.db"


class DataAccessTester:
    """Test harness for data access classes"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        # Initialize all DAOs
        self.patient_dao = PatientDataAccess(db_path)
        self.lab_dao = LabDataAccess(db_path)
        self.treatment_dao = DimTreatmentDataAccess(db_path)
        self.admin_type_dao = DimAdministrationTypeDataAccess(db_path)
        self.treatment_access_dao = TreatmentDataAccess(db_path)

        # Store for created IDs
        self.patient_ids = []
        self.lab_ids = []
        self.treatment_ids = []

    def setup_database(self):
        """Create a fresh test database"""
        # Remove existing database if it exists
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
                print(f"Removed existing test database: {self.db_path}")
            except Exception as e:
                print(f"Warning: Could not remove existing database: {e}")

        # Create all tables
        print("\n=== Creating Tables ===")
        results = [
            self._execute_and_report(
                "Patient table", lambda: self.patient_dao.create_table()
            ),
            self._execute_and_report(
                "Lab results table", lambda: self.lab_dao.create_table()
            ),
            self._execute_and_report(
                "Treatment dimension table", lambda: self.treatment_dao.create_table()
            ),
            self._execute_and_report(
                "Administration type dimension table",
                lambda: self.admin_type_dao.create_table(),
            ),
            self._execute_and_report(
                "Treatment table", lambda: self.treatment_access_dao.create_table()
            ),
        ]
        return all(r.is_ok() for r in results)

    def _execute_and_report(self, operation_name: str, operation):
        """Execute an operation and report the result"""
        print(f"Testing: {operation_name}...")
        result = handle_database_operation(operation)
        if result.is_ok():
            print(f"✅ {operation_name} succeeded")
            return result
        else:
            print(f"❌ {operation_name} failed: {result.error}")
            return result

    def populate_dimension_tables(self):
        """Populate dimension tables with sample data using direct SQL"""
        print("\n=== Populating Dimension Tables ===")

        # Create a connection for these operations
        with DatabaseConnection(self.db_path) as conn:
            # Add sample treatments
            treatments = [
                (
                    1,
                    "Insulin",
                    "Standard insulin treatment",
                    int(datetime.datetime.now().timestamp() * 1000),
                    None,
                ),
                (
                    2,
                    "Saline",
                    "IV fluid replacement",
                    int(datetime.datetime.now().timestamp() * 1000),
                    None,
                ),
                (
                    3,
                    "Potassium",
                    "Electrolyte replacement",
                    int(datetime.datetime.now().timestamp() * 1000),
                    None,
                ),
            ]

            try:
                cursor = conn.cursor()
                cursor.executemany(
                    "INSERT OR REPLACE INTO dim_treatment (treatment_id, treatment_name, treatment_description, rec_create_time, rec_modified_time) VALUES (?, ?, ?, ?, ?)",
                    treatments,
                )
                print(f"✅ Added {len(treatments)} sample treatments")

                # Add sample administration types
                admin_types = [
                    (
                        1,
                        "IV",
                        "Intravenous administration",
                        int(datetime.datetime.now().timestamp() * 1000),
                        None,
                    ),
                    (
                        2,
                        "Oral",
                        "Oral administration",
                        int(datetime.datetime.now().timestamp() * 1000),
                        None,
                    ),
                    (
                        3,
                        "Subcutaneous",
                        "Subcutaneous injection",
                        int(datetime.datetime.now().timestamp() * 1000),
                        None,
                    ),
                ]

                cursor.executemany(
                    "INSERT OR REPLACE INTO dim_administration_type (administration_type_id, administration_type_name, administration_type_description, rec_create_time, rec_modified_time) VALUES (?, ?, ?, ?, ?)",
                    admin_types,
                )
                print(f"✅ Added {len(admin_types)} sample administration types")

                return True
            except sqlite3.Error as e:
                print(f"❌ Error populating dimension tables: {e}")
                return False

    def test_patient_operations(self):
        """Test all patient data access operations"""
        print("\n=== Testing Patient Operations ===")

        # Test patient insertion
        patient_data = {
            "hn": f"HN{datetime.datetime.now().timestamp():.0f}",  # Unique HN
            "name": "Test Patient",
            "age": 35,
            "sex": "Female",
            "medical_history": "Test medical history",
        }

        result = self._execute_and_report(
            "Patient insertion", lambda: self.patient_dao.insert(patient_data)
        )

        if result.is_ok():
            patient_id = result.unwrap()
            self.patient_ids.append(patient_id)

            # Test patient retrieval
            get_result = self._execute_and_report(
                "Patient retrieval", lambda: self.patient_dao.get_by_id(patient_id)
            )

            if get_result.is_ok() and get_result.unwrap():
                retrieved_patient = get_result.unwrap()
                print(f"Retrieved patient: {retrieved_patient}")

                # Test patient update
                update_data = {"name": "Updated Test Patient", "age": 36}

                update_result = self._execute_and_report(
                    "Patient update",
                    lambda: self.patient_dao.update(patient_id, update_data),
                )

                if update_result.is_ok():
                    # Verify update
                    verify_result = handle_database_operation(
                        lambda: self.patient_dao.get_by_id(patient_id)
                    )

                    if verify_result.is_ok() and verify_result.unwrap():
                        updated_patient = verify_result.unwrap()
                        print(f"Updated patient: {updated_patient}")

                        # Verify data was updated correctly
                        assert updated_patient["name"] == update_data["name"], (
                            "Name not updated correctly"
                        )
                        assert updated_patient["age"] == update_data["age"], (
                            "Age not updated correctly"
                        )
                        print("✅ Update verified")

            # Test get all patients
            get_all_result = self._execute_and_report(
                "Get all patients", lambda: self.patient_dao.get_all()
            )

            if get_all_result.is_ok():
                all_patients = get_all_result.unwrap()
                print(f"Found {len(all_patients)} patients")

                # Don't delete the patient yet - we'll need it for lab tests

        # Test invalid operations
        print("\n--- Testing Error Handling ---")
        # Try inserting with missing required fields
        invalid_patient = {
            "name": "Invalid Patient",
            # Missing required fields
        }

        invalid_result = handle_database_operation(
            lambda: self.patient_dao.insert(invalid_patient)
        )

        if invalid_result.is_err():
            print(f"✅ Correctly rejected invalid patient data: {invalid_result.error}")
        else:
            print("❌ Failed: Invalid patient data was accepted")

        # Try getting a non-existent patient
        nonexistent_result = handle_database_operation(
            lambda: self.patient_dao.get_by_id(9999)
        )

        if nonexistent_result.is_ok() and nonexistent_result.unwrap() is None:
            print("✅ Correctly handled non-existent patient")
        else:
            print("❌ Failed: Unexpected response for non-existent patient")

    def test_lab_operations(self):
        """Test lab results data access operations"""
        print("\n=== Testing Lab Results Operations ===")

        if not self.patient_ids:
            print("❌ Cannot test lab operations: No patients available")
            return

        # Get a patient ID to use
        patient_id = self.patient_ids[0]

        # Test lab insertion
        now = datetime.datetime.now()
        sampled_time = now - datetime.timedelta(hours=1)
        result_time = now - datetime.timedelta(minutes=30)

        lab_data = {
            "patient_id": patient_id,
            "logtime": int(now.timestamp() * 1000),
            "sampled_time": int(sampled_time.timestamp() * 1000),
            "result_time": int(result_time.timestamp() * 1000),
            "dtx": 110,
            "ph": 7.40,
            "k": 4.5,
            "na": 140,
        }

        result = self._execute_and_report(
            "Lab result insertion", lambda: self.lab_dao.insert(lab_data)
        )

        if result.is_ok():
            lab_id = result.unwrap()
            self.lab_ids.append(lab_id)

            # Test lab retrieval
            get_result = self._execute_and_report(
                "Lab result retrieval", lambda: self.lab_dao.get_by_id(lab_id)
            )

            if get_result.is_ok() and get_result.unwrap():
                retrieved_lab = get_result.unwrap()
                print(f"Retrieved lab result: {retrieved_lab}")

                # Test lab update
                update_data = {"ph": 7.35, "k": 4.2}

                update_result = self._execute_and_report(
                    "Lab result update",
                    lambda: self.lab_dao.update(lab_id, update_data),
                )

                if update_result.is_ok():
                    # Verify update
                    verify_result = handle_database_operation(
                        lambda: self.lab_dao.get_by_id(lab_id)
                    )

                    if verify_result.is_ok() and verify_result.unwrap():
                        updated_lab = verify_result.unwrap()
                        print(f"Updated lab result: {updated_lab}")

                        # Verify data was updated correctly
                        assert updated_lab["ph"] == update_data["ph"], (
                            "pH not updated correctly"
                        )
                        assert updated_lab["k"] == update_data["k"], (
                            "K not updated correctly"
                        )
                        print("✅ Update verified")

            # Test get all lab results
            get_all_result = self._execute_and_report(
                "Get all lab results", lambda: self.lab_dao.get_all()
            )

            if get_all_result.is_ok():
                all_labs = get_all_result.unwrap()
                print(f"Found {len(all_labs)} lab results")

    def test_treatment_operations(self):
        """Test treatment data access operations"""
        print("\n=== Testing Treatment Operations ===")

        if not self.patient_ids:
            print("❌ Cannot test treatment operations: No patients available")
            return

        # Get a patient ID to use
        patient_id = self.patient_ids[0]

        # Test treatment insertion
        now = datetime.datetime.now()
        administered_time = now - datetime.timedelta(hours=2)
        end_time = now - datetime.timedelta(hours=1)

        treatment_data = {
            "patient_id": patient_id,
            "logtime": int(now.timestamp() * 1000),
            "administored_time": int(administered_time.timestamp() * 1000),
            "end_time": int(end_time.timestamp() * 1000),
            "treatment_id": 1,  # Insulin from dimension table
            "application_method_id": 1,  # IV from dimension table
            "administration_rate": 2,  # Units per hour
        }

        result = self._execute_and_report(
            "Treatment insertion",
            lambda: self.treatment_access_dao.insert(treatment_data),
        )

        if result.is_ok():
            treatment_id = result.unwrap()
            self.treatment_ids.append(treatment_id)

            # Test treatment retrieval
            get_result = self._execute_and_report(
                "Treatment retrieval",
                lambda: self.treatment_access_dao.get_by_id(treatment_id),
            )

            if get_result.is_ok() and get_result.unwrap():
                retrieved_treatment = get_result.unwrap()
                print(f"Retrieved treatment: {retrieved_treatment}")

                # Test treatment update
                update_data = {
                    "administration_rate": 3,  # Updated rate
                    "end_time": int(
                        (now - datetime.timedelta(minutes=30)).timestamp() * 1000
                    ),  # Updated end time
                }

                update_result = self._execute_and_report(
                    "Treatment update",
                    lambda: self.treatment_access_dao.update(treatment_id, update_data),
                )

                if update_result.is_ok():
                    # Verify update
                    verify_result = handle_database_operation(
                        lambda: self.treatment_access_dao.get_by_id(treatment_id)
                    )

                    if verify_result.is_ok() and verify_result.unwrap():
                        updated_treatment = verify_result.unwrap()
                        print(f"Updated treatment: {updated_treatment}")

                        # Verify data was updated correctly
                        assert (
                            updated_treatment["administration_rate"]
                            == update_data["administration_rate"]
                        ), "Administration rate not updated correctly"
                        assert (
                            updated_treatment["end_time"] == update_data["end_time"]
                        ), "End time not updated correctly"
                        print("✅ Update verified")

            # Test get all treatments
            get_all_result = self._execute_and_report(
                "Get all treatments", lambda: self.treatment_access_dao.get_all()
            )

            if get_all_result.is_ok():
                all_treatments = get_all_result.unwrap()
                print(f"Found {len(all_treatments)} treatments")

        # Test invalid treatment data
        print("\n--- Testing Error Handling for Treatments ---")
        # Try inserting with missing required fields
        invalid_treatment = {
            "patient_id": patient_id,
            "logtime": int(now.timestamp() * 1000),
            # Missing other required fields
        }

        invalid_result = handle_database_operation(
            lambda: self.treatment_access_dao.insert(invalid_treatment)
        )

        if invalid_result.is_err():
            print(
                f"✅ Correctly rejected invalid treatment data: {invalid_result.error}"
            )
        else:
            print("❌ Failed: Invalid treatment data was accepted")

    def test_dimension_table_operations(self):
        """Test dimension table operations"""
        print("\n=== Testing Dimension Table Operations ===")

        # Test treatment retrieval
        treatment_result = self._execute_and_report(
            "Treatment retrieval", lambda: self.treatment_dao.get_by_id(1)
        )

        if treatment_result.is_ok():
            treatment = treatment_result.unwrap()
            if treatment:
                print(f"Retrieved treatment: {treatment}")
            else:
                print("Treatment with ID 1 not found")

        # Test administration type retrieval
        admin_type_result = self._execute_and_report(
            "Administration type retrieval", lambda: self.admin_type_dao.get_by_id(1)
        )

        if admin_type_result.is_ok():
            admin_type = admin_type_result.unwrap()
            if admin_type:
                print(f"Retrieved administration type: {admin_type}")
            else:
                print("Administration type with ID 1 not found")

        # Test get all treatments
        get_all_treatments_result = self._execute_and_report(
            "Get all treatments", lambda: self.treatment_dao.get_all()
        )

        if get_all_treatments_result.is_ok():
            all_treatments = get_all_treatments_result.unwrap()
            print(f"Found {len(all_treatments)} treatments")

        # Test get all administration types
        get_all_admin_types_result = self._execute_and_report(
            "Get all administration types", lambda: self.admin_type_dao.get_all()
        )

        if get_all_admin_types_result.is_ok():
            all_admin_types = get_all_admin_types_result.unwrap()
            print(f"Found {len(all_admin_types)} administration types")

        # Test that insert/update/delete operations are prohibited
        print("\n--- Testing Dimension Table Protection ---")
        try:
            # Attempt to insert a treatment
            insert_result = handle_database_operation(
                lambda: self.treatment_dao.insert({"treatment_name": "Test Treatment"})
            )

            if insert_result.is_err():
                print("✅ Correctly prevented insertion into dimension table")
            else:
                print("❌ Failed: Was able to insert into dimension table")
        except NotImplementedError:
            print("✅ Correctly raised NotImplementedError for dimension table insert")

    def test_deletion_operations(self):
        """Test deletion operations"""
        print("\n=== Testing Deletion Operations ===")

        # Delete treatments first (due to patient foreign key)
        for treatment_id in self.treatment_ids:
            delete_result = self._execute_and_report(
                f"Delete treatment {treatment_id}",
                lambda: self.treatment_access_dao.delete(treatment_id),
            )

            if delete_result.is_ok():
                # Verify deletion
                verify_result = handle_database_operation(
                    lambda: self.treatment_access_dao.get_by_id(treatment_id)
                )

                if verify_result.is_ok() and verify_result.unwrap() is None:
                    print(f"✅ Verified treatment {treatment_id} was deleted")
                else:
                    print(
                        f"❌ Failed: Treatment {treatment_id} still exists after deletion"
                    )

        # Delete lab results next (due to foreign key constraints)
        for lab_id in self.lab_ids:
            delete_result = self._execute_and_report(
                f"Delete lab result {lab_id}", lambda: self.lab_dao.delete(lab_id)
            )

            if delete_result.is_ok():
                # Verify deletion
                verify_result = handle_database_operation(
                    lambda: self.lab_dao.get_by_id(lab_id)
                )

                if verify_result.is_ok() and verify_result.unwrap() is None:
                    print(f"✅ Verified lab result {lab_id} was deleted")
                else:
                    print(f"❌ Failed: Lab result {lab_id} still exists after deletion")

        # Delete patients
        for patient_id in self.patient_ids:
            delete_result = self._execute_and_report(
                f"Delete patient {patient_id}",
                lambda: self.patient_dao.delete(patient_id),
            )

            if delete_result.is_ok():
                # Verify deletion
                verify_result = handle_database_operation(
                    lambda: self.patient_dao.get_by_id(patient_id)
                )

                if verify_result.is_ok() and verify_result.unwrap() is None:
                    print(f"✅ Verified patient {patient_id} was deleted")
                else:
                    print(
                        f"❌ Failed: Patient {patient_id} still exists after deletion"
                    )

    def close_connections(self):
        """Close all DAO connections"""
        print("\n=== Closing Connections ===")
        for dao in [
            self.patient_dao,
            self.lab_dao,
            self.treatment_dao,
            self.admin_type_dao,
            self.treatment_access_dao,
        ]:
            if dao.check_connection():
                dao._close_connection()
                print(f"Closed connection for {dao.__class__.__name__}")
            else:
                print(f"Connection already closed for {dao.__class__.__name__}")

    def run_all_tests(self):
        """Run all tests in sequence"""
        try:
            if not self.setup_database():
                print("❌ Database setup failed, aborting tests")
                return

            if not self.populate_dimension_tables():
                print("❌ Dimension table population failed, continuing with caution")

            self.test_patient_operations()
            self.test_lab_operations()
            self.test_treatment_operations()  # New test for treatments
            self.test_dimension_table_operations()
            self.test_deletion_operations()

            print("\n=== Test Summary ===")
            print(f"Tested with database: {self.db_path}")
            print("All tests completed.")

        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            traceback.print_exc()
        finally:
            self.close_connections()


if __name__ == "__main__":
    print("=== SQLite Data Access Layer Testing ===")
    print(f"Testing against database: {TEST_DB_PATH}")

    tester = DataAccessTester(TEST_DB_PATH)
    tester.run_all_tests()

    print("\nTests completed.")
