from typing import Optional, List, Dict, Any
from src.logic.data_access_util import (
    TransactionalDataAccess,
    DimensionalDataAccess,
)


class DimTreatmentDataAccess(DimensionalDataAccess):
    """
    Data access class for the 'dim_treatment' dimension table.
    Inherits from DimensionalDataAccess, which provides basic read-only
    functionality and table creation.
    """

    def __init__(self, db_path: str):
        """
        Initializes the DimTreatmentDataAccess object.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        table_name = "dim_treatment"
        column_definitions = [
            "treatment_id INTEGER PRIMARY KEY",
            "treatment_name TEXT NOT NULL",
            "treatment_description TEXT",
            "rec_create_time BIGINT",  # unixtime when rec is created
            "rec_modified_time BIGINT",  # unixtime when rec is modified
        ]
        super().__init__(db_path, table_name, column_definitions)
        self.id_column_name = "treatment_id"

    def get_by_id(self, treatment_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a treatment record by its ID."""
        return super().get_by_id(treatment_id)  # Use the base class method

    def get_all(self) -> List[Dict[str, Any]]:
        """Retrieves all treatment records from the table."""
        return super().get_all()  # Use the base class method
    
class DimAdministrationTypeDataAccess(DimensionalDataAccess):
    """
    Data access class for the 'dim_administration_type' dimension table.
    Inherits from DimensionalDataAccess, which provides basic read-only
    functionality and table creation.
    """

    def __init__(self, db_path: str):
        """
        Initializes the DimAdministrationTypeDataAccess object.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        table_name = "dim_administration_type"
        column_definitions = [
            "administration_type_id INTEGER PRIMARY KEY",
            "administration_type_name TEXT NOT NULL",
            "administration_type_description TEXT",
            "rec_create_time BIGINT",  # unixtime when record is created
            "rec_modified_time BIGINT",  # unixtime when record is modified
        ]
        super().__init__(db_path, table_name, column_definitions)
        self.id_column_name = "administration_type_id" 

    def get_by_id(self, administration_type_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves an administration type record by its ID."""
        return super().get_by_id(administration_type_id)

    def get_all(self) -> List[Dict[str, Any]]:
        """Retrieves all administration type records from the table."""
        return super().get_all()


class TreatmentDataAccess(TransactionalDataAccess):
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
        table_name = "treatment"
        column_definitions = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "patient_id INTEGER",
            "logtime BIGINT NOT NULL",  # unixtime (ms) where data got added to the table
            "administored_time BIGINT NOT NULL",  # unixtime (ms) when blood sample is being taken
            "end_time BIGINT NOT NULL",  # unixtime (ms) when lab result is available
            "treatment_id INT NOT NULL",  # treatment administored from dim_treatment table
            "application_method_id INT NOT NULL",  # application method id from dim_application table
            "administration_rate INT",  # rate of which treatment is being administored
            "FOREIGN KEY (treatment_id) REFERENCES dim_treatment(treatment_id) ON UPDATE CASCADE ON DELETE CASCADE",
            "FOREIGN KEY (application_method_id) REFERENCES dim_application_method(application_method_id) ON UPDATE CASCADE ON DELETE CASCADE",
        ]
        super().__init__(db_path, table_name, column_definitions)
        self.id_column_name = "id"

    def create_table(self) -> None:
        pass

    def insert(self, data: Dict[str, Any]) -> int:
        """Inserts a new treatment result record."""
        required_keys = {
            "patient_id",
            "logtime",
            "administored_time",
            "end_time",
            "treatment_id",
            "application_method_id",
            "administration_rate",
        }
        if not required_keys.issubset(data.keys()):
            missing = required_keys - data.keys()
            raise ValueError(f"Missing required keys for insert: {missing}")

        # Validate 'administration_rate' value
        if "administration_rate" in data and data["administration_rate"] is not None and data["administration_rate"] < 0:
            raise ValueError("'administration_rate' value cannot be negative.")

        insert_data = {
            "patient_id": data["patient_id"],
            "logtime": data["logtime"],
            "administored_time": data["administored_time"],
            "end_time": data["end_time"],
            "treatment_id": data["treatment_id"],
            "application_method_id": data["application_method_id"],
            "administration_rate": data["administration_rate"],
        }
        return super().insert(insert_data)

    def get_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a treament result by its primary key ID."""
        return super().get_by_id(record_id)

    def update(self, record_id: int, data: Dict[str, Any]) -> bool:
        """Updates an existing treament result record."""
        if not data:
            raise ValueError("No data provided for update.")

        update_data = {
            "patient_id": data.get("patient_id"),
            "logtime": data.get("logtime"),
            "administored_time": data.get("administored_time"),
            "end_time": data.get("end_time"),
            "treatment_id": data.get("treatment_id"),
            "application_method_id": data.get("application_method_id"),
            "administration_rate": data.get("administration_rate"),
        }
        update_data = {k: v for k, v in update_data.items() if v is not None}
        return super().update(record_id, update_data)

    def delete(self, record_id: int) -> bool:
        """Deletes a lab result record by its ID."""
        return super().delete(record_id)

    def get_all(self) -> List[Dict[str, Any]]:
        """Retrieves all lab result records."""
        return super().get_all()
