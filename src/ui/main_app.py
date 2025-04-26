import streamlit as st
import os

# Ensure paths are relative to the current file's directory
base_path = os.path.dirname(__file__)

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