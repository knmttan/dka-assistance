import streamlit as st
from src.logic.patient_data_access import PatientDataAccess

def add_patient():
    st.header("Add New Patient")

    # Input fields for patient details
    hn = st.text_input("Hospital Number (HN)")
    name = st.text_input("Name")
    age = st.number_input("Age", min_value=0, step=1)
    sex = st.selectbox("Sex", ["Male", "Female"])
    medical_history = st.text_area("Medical History")

    if st.button("Add Patient"):
        try:
            db_path = "./src/data/dka_data.db"
            patient_dao = PatientDataAccess(db_path)
            patient_data = {
                "hn": hn,
                "name": name,
                "age": age,
                "sex": sex,
                "medical_history": medical_history,
            }
            patient_dao.insert(patient_data)
            st.success("Patient added successfully!")
        except Exception as e:
            st.error(f"Error: {e}")