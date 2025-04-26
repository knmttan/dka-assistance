import sqlite3

# Initialize the database and create tables
def initialize_database():
    db_path = "./src/data/dka_data.db"
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # Create patients table
    # patients table columns:
    # - patient_id: INTEGER PRIMARY KEY AUTOINCREMENT
    # - hn: TEXT NOT NULL UNIQUE
    # - name: TEXT NOT NULL
    # - age: INTEGER NOT NULL
    # - sex: TEXT NOT NULL
    # - medical_history: TEXT
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS patients (
            patient_id INTEGER PRIMARY KEY AUTOINCREMENT, -- Primary key
            hn TEXT NOT NULL UNIQUE, -- Hospital number, unique identifier
            name TEXT NOT NULL, -- Patient's name
            age INTEGER NOT NULL, -- Patient's age
            sex TEXT NOT NULL, -- Patient's sex
            medical_history TEXT -- Patient's medical history
        )
        """
    )

    # Create lab_results table
    # lab_results table columns:
    # - id: INTEGER PRIMARY KEY AUTOINCREMENT
    # - patient_id: INTEGER
    # - logtime: BIGINT NOT NULL (Timestamp where data got added to the table)
    # - sampled_time: BIGINT NOT NULL (Datetime when blood sample is being taken)
    # - result_time: BIGINT NOT NULL (Datetime when lab result is available)
    # - dtx: INT (Dextrostix (mg/dL) measurement value by fingertip)
    # - ph: DOUBLE (pH level of the blood)
    # - k: DOUBLE (Potassium level in the blood (mmol/L || mEq/L))
    # - na: DOUBLE (Sodium level in the blood (mmol/L || mEq/L))
    # - ag: DOUBLE (Anion gap (mmol/L))
    # - ketone: DOUBLE (Ketone level (mmol/L))
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS lab_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT, -- Primary key
            patient_id INTEGER, -- Foreign key to patients table
            logtime BIGINT NOT NULL, -- Timestamp where data got added to the table
            sampled_time BIGINT NOT NULL, -- Datetime when blood sample is being taken
            result_time BIGINT NOT NULL, -- Datetime when lab result is available
            dtx INT, -- Dextrostix (mg/dL) measurement value by fingertip
            ph DOUBLE, -- pH level of the blood
            k DOUBLE, -- Potassium level in the blood (mmol/L || mEq/L)
            na DOUBLE, -- Sodium level in the blood (mmol/L || mEq/L)
            ag DOUBLE, -- Anion gap (mmol/L)
            ketone DOUBLE, -- Ketone level (mmol/L)
            FOREIGN KEY (patient_id) REFERENCES patients(patient_id) ON UPDATE CASCADE ON DELETE CASCADE -- Foreign key constraint
        )
        """
    )

    # Create treatment table
    # treatment table columns:
    # - id: INTEGER PRIMARY KEY AUTOINCREMENT
    # - patient_id: INTEGER
    # - logtime: BIGINT NOT NULL (unixtime (ms) where data got added to the table)
    # - administored_time: BIGINT NOT NULL (unixtime (ms) when blood sample is being taken)
    # - end_time: BIGINT NOT NULL (unixtime (ms) when lab result is available)
    # - treatment_id: INT NOT NULL (treatment administored from dim_treatment table)
    # - application_method_id: INT NOT NULL (application method id from dim_application table)
    # - administration_rate: INT (rate of which treatment is being administored)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS treatment (
            id INTEGER PRIMARY KEY AUTOINCREMENT, -- Primary key
            patient_id INTEGER, -- Foreign key to patients table
            logtime BIGINT NOT NULL, -- Unix timestamp (ms) when data got added to the table
            administored_time BIGINT NOT NULL, -- Unix timestamp (ms) when treatment was administered
            end_time BIGINT NOT NULL, -- Unix timestamp (ms) when treatment ended
            treatment_id INT NOT NULL, -- Foreign key to dim_treatment table
            application_method_id INT NOT NULL, -- Foreign key to dim_application_method table
            administration_rate INT, -- Rate of administration
            FOREIGN KEY (treatment_id) REFERENCES dim_treatment(treatment_id) ON UPDATE CASCADE ON DELETE CASCADE, -- Foreign key constraint
            FOREIGN KEY (application_method_id) REFERENCES dim_application_method(application_method_id) ON UPDATE CASCADE ON DELETE CASCADE -- Foreign key constraint
        )
        """
    )

    # Create dim_treatment table
    # dim_treatment table columns:
    # - treatment_id: INTEGER PRIMARY KEY
    # - treatment_name: TEXT NOT NULL
    # - treatment_description: TEXT
    # - rec_create_time: BIGINT (unixtime when record is created)
    # - rec_modified_time: BIGINT (unixtime when record is modified)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_treatment (
            treatment_id INTEGER PRIMARY KEY, -- Primary key
            treatment_name TEXT NOT NULL, -- Name of the treatment
            treatment_description TEXT, -- Description of the treatment
            rec_create_time BIGINT, -- Unix timestamp when record is created
            rec_modified_time BIGINT -- Unix timestamp when record is modified
        )
        """
    )

    # Insert initial data into dim_treatment table
    cursor.executemany(
        """
        INSERT OR IGNORE INTO dim_treatment (treatment_id, treatment_name, treatment_description, rec_create_time, rec_modified_time)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (1, "Insulin Therapy", "Administering insulin to control blood sugar levels", 0, 0),
            (2, "Fluid Replacement", "Replenishing lost fluids", 0, 0),
            (3, "Electrolyte Replacement", "Restoring electrolyte balance", 0, 0),
        ]
    )

    # Create dim_administration_type table
    # dim_administration_type table columns:
    # - administration_type_id: INTEGER PRIMARY KEY
    # - administration_type_name: TEXT NOT NULL
    # - administration_type_description: TEXT
    # - rec_create_time: BIGINT (unixtime when record is created)
    # - rec_modified_time: BIGINT (unixtime when record is modified)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_administration_type (
            administration_type_id INTEGER PRIMARY KEY, -- Primary key
            administration_type_name TEXT NOT NULL, -- Name of the administration type
            administration_type_description TEXT, -- Description of the administration type
            rec_create_time BIGINT, -- Unix timestamp when record is created
            rec_modified_time BIGINT -- Unix timestamp when record is modified
        )
        """
    )

    # Insert initial data into dim_administration_type table
    cursor.executemany(
        """
        INSERT OR IGNORE INTO dim_administration_type (administration_type_id, administration_type_name, administration_type_description, rec_create_time, rec_modified_time)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (1, "IV_1", "Intravenous method 1", 0, 0),
            (2, "IV_2", "Intravenous method 2", 0, 0),
            (3, "IV_3", "Intravenous method 3", 0, 0),
            (4, "IV_4", "Intravenous method 4", 0, 0),
        ]
    )

    connection.commit()
    connection.close()

if __name__ == "__main__":
    initialize_database()
    print("Database initialized and tables created successfully.")