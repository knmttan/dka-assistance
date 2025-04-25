-- SQL script to initialize dimension tables

-- Create and populate dim_treatment table
CREATE TABLE IF NOT EXISTS dim_treatment (
    treatment_id INTEGER PRIMARY KEY,
    treatment_name TEXT NOT NULL
);

INSERT INTO dim_treatment (treatment_id, treatment_name) VALUES
    (1, 'Insulin Therapy'),
    (2, 'Fluid Replacement'),
    (3, 'Electrolyte Replacement');

-- Create and populate dim_administration_type table
CREATE TABLE IF NOT EXISTS dim_administration_type (
    administration_type_id INTEGER PRIMARY KEY,
    administration_type_name TEXT NOT NULL
);

INSERT INTO dim_administration_type (administration_type_id, administration_type_name) VALUES
    (1, 'IV_1'),
    (2, 'IV_2'),
    (3, 'IV_3'),
    (4, 'IV_4')
;