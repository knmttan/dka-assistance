import sys
import os

# Add the parent directory of src to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st

# Ensure paths are relative to the current file's directory
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "ui"))

pages = {
    "Patient Management": [
        st.Page(os.path.join(base_path, "add_patient.py"), title="Add Patient"),
    ],
    "Lab Results": [
        st.Page(os.path.join(base_path, "add_lab_result.py"), title="Add Lab Result"),
    ],
    "Treatments": [
        st.Page(os.path.join(base_path, "add_treatment.py"), title="Add Administered Treatment"),
    ],
}

pg = st.navigation(pages)
pg.run()