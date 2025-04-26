import streamlit as st
from src.ui.add_patient import add_patient
from src.ui.add_treatment import suggest_treatment
from src.ui.add_lab_of_patient import patient_and_lab_details


def home():
    st.title("Welcome to the DKA Assistance App")
    st.write("This application helps manage patient data, lab results, and treatments.")


# Initialize session state for shared data
if "shared_data" not in st.session_state:
    st.session_state["shared_data"] = {}

# Define pages for navigation using st.Page
pages = [
    st.Page(home, title="Home", icon="🏠"),
    st.Page(patient_and_lab_details, title="Patient and Lab Details", icon="🧑‍⚕️"),
    st.Page(suggest_treatment, title="Treatment Suggestions", icon="💊"),
    st.Page(add_patient, title="Add Patient", icon="➕")
]

# Use st.navigation to manage navigation
pg = st.navigation(pages)
pg.run()
