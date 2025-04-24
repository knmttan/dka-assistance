# dka-assistance
A proof of concept website providing resources and tools to assist doctors in the management of Diabetic Ketoacidosis (DKA) in patients.

repo structure:
```plaintext
dka-assistance/
├── .streamlit/
│   └── config.toml
├── src/
│   ├── __init__.py
│   ├── ui/
│   │   ├── __init__.py
│   │   ├── main_app.py
│   │   ├── patient_profile.py
│   │   ├── management_view.py
│   │   ├── input_fields.py
│   │   └── output_display.py
│   ├── logic/
│   │   ├── __init__.py
│   │   ├── patient_logic.py
│   │   ├── dka_calculations.py
│   │   └── data_access.py
│   ├── data/
│   │   └── dka_data.db
├── requirements.txt
├── README.md
├── LICENSE
└── tests/
    ├── __init__.py
    └── test_dka_calculations.py
```