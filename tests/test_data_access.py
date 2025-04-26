import logging
import datetime
import pytest
import subprocess
import os
import sqlite3

from src.logic.lab_data_access import LabDataAccess
from src.logic.patient_data_access import PatientDataAccess
from src.logic.treatment_data_access import (
    TreatmentDataAccess,
    DimTreatmentDataAccess,
    DimAdministrationTypeDataAccess,
)


# Check if the database file exists, disconnect and delete it
@pytest.fixture(scope="module", autouse=True)
def cleanup_database():
    db_path = "./src/data/dka_data.db"
    if os.path.exists(db_path):
        try:
            connection = sqlite3.connect(db_path)
            connection.close()
        except Exception as e:
            print(f"Error disconnecting database: {e}")
        os.remove(db_path)
        print(f"Database file {db_path} deleted.")
    
    # Ensure database is initialized after cleanup
    subprocess.run(["python", "./src/initialize_database.py"], check=True)

# Configure logging to capture test output
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)



@pytest.fixture(scope="module")
def setup_database():
    db_path = "./src/data/dka_data.db"
    return db_path

@pytest.fixture(scope="module")
def daos(setup_database):
    db_path = setup_database
    patient_dao = PatientDataAccess(db_path)
    lab_dao = LabDataAccess(db_path)
    treatment_dao = TreatmentDataAccess(db_path)
    dim_treatment_dao = DimTreatmentDataAccess(db_path)
    dim_administration_type_dao = DimAdministrationTypeDataAccess(db_path)

    yield {
        "patient_dao": patient_dao,
        "lab_dao": lab_dao,
        "treatment_dao": treatment_dao,
        "dim_treatment_dao": dim_treatment_dao,
        "dim_administration_type_dao": dim_administration_type_dao,
    }

    # Cleanup
    for dao in [patient_dao, lab_dao, treatment_dao, dim_treatment_dao, dim_administration_type_dao]:
        if dao.check_connection():
            dao._close_connection()

# Test patient CRUD operations
def test_patient_crud_operations(daos):
    patient_dao = daos["patient_dao"]

    # Insert a patient
    patient_data = {
        "hn": "HN12345",
        "name": "John Doe",
        "age": 30,
        "sex": "Male",
        "medical_history": "None",
    }
    patient_id = patient_dao.insert(patient_data)
    logger.info(f"Inserted patient with ID: {patient_id}")
    assert patient_id is not None

    # Retrieve the patient
    retrieved_patient = patient_dao.get_by_id(patient_id)
    logger.info(f"Retrieved patient: {retrieved_patient}")
    assert retrieved_patient is not None
    assert retrieved_patient["name"] == "John Doe"

    # Update the patient
    update_data = {"name": "John Smith", "age": 31}
    update_status = patient_dao.update(patient_id, update_data)
    logger.info(f"Update status: {update_status}")
    assert update_status

    # Verify the update
    updated_patient = patient_dao.get_by_id(patient_id)
    logger.info(f"Updated patient: {updated_patient}")
    assert updated_patient["name"] == "John Smith"
    assert updated_patient["age"] == 31

    # Delete the patient
    delete_status = patient_dao.delete(patient_id)
    logger.info(f"Delete status: {delete_status}")
    assert delete_status

    # Verify deletion
    deleted_patient = patient_dao.get_by_id(patient_id)
    logger.info(f"Deleted patient: {deleted_patient}")
    assert deleted_patient is None

# Test lab CRUD operations
def test_lab_crud_operations(daos):
    lab_dao = daos["lab_dao"]

    # Insert a lab result
    now = datetime.datetime.now()
    lab_data = {
        "patient_id": 1,
        "logtime": int(now.timestamp() * 1000),
        "sampled_time": int((now - datetime.timedelta(hours=1)).timestamp() * 1000),
        "result_time": int((now - datetime.timedelta(minutes=30)).timestamp() * 1000),
        "dtx": 120,
        "ph": 7.4,
        "k": 4.5,
        "na": 140,
        "ag": 12.5,
        "ketone": 1.2,
    }
    lab_id = lab_dao.insert(lab_data)
    logger.info(f"Inserted lab result with ID: {lab_id}")
    assert lab_id is not None

    # Retrieve the lab result
    retrieved_lab = lab_dao.get_by_id(lab_id)
    logger.info(f"Retrieved lab result: {retrieved_lab}")
    assert retrieved_lab is not None
    assert retrieved_lab["dtx"] == 120
    assert retrieved_lab["ag"] == 12.5
    assert retrieved_lab["ketone"] == 1.2

    # Update the lab result
    update_data = {"ph": 7.35, "k": 4.2, "ag": 13.0, "ketone": 1.5}
    update_status = lab_dao.update(lab_id, update_data)
    logger.info(f"Update status: {update_status}")
    assert update_status

    # Verify the update
    updated_lab = lab_dao.get_by_id(lab_id)
    logger.info(f"Updated lab result: {updated_lab}")
    assert updated_lab["ph"] == 7.35
    assert updated_lab["k"] == 4.2
    assert updated_lab["ag"] == 13.0
    assert updated_lab["ketone"] == 1.5

    # Delete the lab result
    delete_status = lab_dao.delete(lab_id)
    logger.info(f"Delete status: {delete_status}")
    assert delete_status

    # Verify deletion
    deleted_lab = lab_dao.get_by_id(lab_id)
    logger.info(f"Deleted lab result: {deleted_lab}")
    assert deleted_lab is None

# Test treatment CRUD operations
def test_treatment_crud_operations(daos):
    treatment_dao = daos["treatment_dao"]

    # Insert a treatment
    now = datetime.datetime.now()
    treatment_data = {
        "patient_id": 1,
        "logtime": int(now.timestamp() * 1000),
        "administored_time": int((now - datetime.timedelta(hours=2)).timestamp() * 1000),
        "end_time": int((now - datetime.timedelta(hours=1)).timestamp() * 1000),
        "treatment_id": 1,
        "application_method_id": 1,
        "administration_rate": 2,
    }
    treatment_id = treatment_dao.insert(treatment_data)
    logger.info(f"Inserted treatment with ID: {treatment_id}")
    assert treatment_id is not None

    # Retrieve the treatment
    retrieved_treatment = treatment_dao.get_by_id(treatment_id)
    logger.info(f"Retrieved treatment: {retrieved_treatment}")
    assert retrieved_treatment is not None
    assert retrieved_treatment["administration_rate"] == 2

    # Update the treatment
    update_data = {"administration_rate": 3}
    update_status = treatment_dao.update(treatment_id, update_data)
    logger.info(f"Update status: {update_status}")
    assert update_status

    # Verify the update
    updated_treatment = treatment_dao.get_by_id(treatment_id)
    logger.info(f"Updated treatment: {updated_treatment}")
    assert updated_treatment["administration_rate"] == 3

    # Delete the treatment
    delete_status = treatment_dao.delete(treatment_id)
    logger.info(f"Delete status: {delete_status}")
    assert delete_status

    # Verify deletion
    deleted_treatment = treatment_dao.get_by_id(treatment_id)
    logger.info(f"Deleted treatment: {deleted_treatment}")
    assert deleted_treatment is None

# Additional test cases to cover more scenarios

# Test inserting a patient with missing fields
def test_patient_insert_missing_fields(daos):
    patient_dao = daos["patient_dao"]

    # Missing 'name' field
    patient_data = {
        "hn": "HN67890",
        "age": 25,
        "sex": "Female",
        "medical_history": "Asthma",
    }
    with pytest.raises(Exception):
        patient_dao.insert(patient_data)

# Test retrieving a non-existent patient
def test_patient_retrieve_non_existent(daos):
    patient_dao = daos["patient_dao"]

    # Attempt to retrieve a patient with an invalid ID
    retrieved_patient = patient_dao.get_by_id(9999)
    assert retrieved_patient is None

# Test updating a non-existent patient
def test_patient_update_non_existent(daos):
    patient_dao = daos["patient_dao"]

    # Attempt to update a patient with an invalid ID
    update_data = {"name": "Non Existent", "age": 40}
    update_status = patient_dao.update(9999, update_data)
    assert not update_status

# Test deleting a non-existent patient
def test_patient_delete_non_existent(daos):
    patient_dao = daos["patient_dao"]

    # Attempt to delete a patient with an invalid ID
    delete_status = patient_dao.delete(9999)
    assert not delete_status

# Test inserting a lab result with invalid data
def test_lab_insert_invalid_data(daos):
    lab_dao = daos["lab_dao"]

    # Invalid 'dtx' value (negative)
    lab_data = {
        "patient_id": 1,
        "logtime": int(datetime.datetime.now().timestamp() * 1000),
        "sampled_time": int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp() * 1000),
        "result_time": int((datetime.datetime.now() - datetime.timedelta(minutes=30)).timestamp() * 1000),
        "dtx": -10,
        "ph": 7.4,
        "k": 4.5,
        "na": 140,
    }
    with pytest.raises(Exception):
        lab_dao.insert(lab_data)

# Test retrieving a non-existent lab result
def test_lab_retrieve_non_existent(daos):
    lab_dao = daos["lab_dao"]

    # Attempt to retrieve a lab result with an invalid ID
    retrieved_lab = lab_dao.get_by_id(9999)
    assert retrieved_lab is None

# Test updating a non-existent lab result
def test_lab_update_non_existent(daos):
    lab_dao = daos["lab_dao"]

    # Attempt to update a lab result with an invalid ID
    update_data = {"ph": 7.2, "k": 4.0}
    update_status = lab_dao.update(9999, update_data)
    assert not update_status

# Test deleting a non-existent lab result
def test_lab_delete_non_existent(daos):
    lab_dao = daos["lab_dao"]

    # Attempt to delete a lab result with an invalid ID
    delete_status = lab_dao.delete(9999)
    assert not delete_status

# Test inserting a treatment with invalid data
def test_treatment_insert_invalid_data(daos):
    treatment_dao = daos["treatment_dao"]

    # Invalid 'administration_rate' value (negative)
    treatment_data = {
        "patient_id": 1,
        "logtime": int(datetime.datetime.now().timestamp() * 1000),
        "administored_time": int((datetime.datetime.now() - datetime.timedelta(hours=2)).timestamp() * 1000),
        "end_time": int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp() * 1000),
        "treatment_id": 1,
        "application_method_id": 1,
        "administration_rate": -5,
    }
    with pytest.raises(Exception):
        treatment_dao.insert(treatment_data)

# Test retrieving a non-existent treatment
def test_treatment_retrieve_non_existent(daos):
    treatment_dao = daos["treatment_dao"]

    # Attempt to retrieve a treatment with an invalid ID
    retrieved_treatment = treatment_dao.get_by_id(9999)
    assert retrieved_treatment is None

# Test updating a non-existent treatment
def test_treatment_update_non_existent(daos):
    treatment_dao = daos["treatment_dao"]

    # Attempt to update a treatment with an invalid ID
    update_data = {"administration_rate": 10}
    update_status = treatment_dao.update(9999, update_data)
    assert not update_status

# Test deleting a non-existent treatment
def test_treatment_delete_non_existent(daos):
    treatment_dao = daos["treatment_dao"]

    # Attempt to delete a treatment with an invalid ID
    delete_status = treatment_dao.delete(9999)
    assert not delete_status

# Test cases for dimension tables

def test_dim_treatment_initialization(daos):
    dim_treatment_dao = daos["dim_treatment_dao"]

    # Verify the dim_treatment table contains the expected data
    treatments = dim_treatment_dao.get_all()
    assert len(treatments) == 3
    assert treatments[0]["treatment_name"] == "Insulin Therapy"
    assert treatments[1]["treatment_name"] == "Fluid Replacement"
    assert treatments[2]["treatment_name"] == "Electrolyte Replacement"

def test_dim_administration_type_initialization(daos):
    dim_administration_type_dao = daos["dim_administration_type_dao"]

    # Verify the dim_administration_type table contains the expected data
    admin_types = dim_administration_type_dao.get_all()
    assert len(admin_types) == 4
    assert admin_types[0]["administration_type_name"] == "IV_1"
    assert admin_types[1]["administration_type_name"] == "IV_2"
    assert admin_types[2]["administration_type_name"] == "IV_3"
    assert admin_types[3]["administration_type_name"] == "IV_4"