import streamlit as st
import time
import datetime
from src.logic.patient_data_access import PatientDataAccess
from src.logic.lab_data_access import LabDataAccess

def patient_and_lab_details():
    st.header("Patient and Lab Details")

    # Select or add a patient
    st.subheader("Select Patient")
    db_path = "./src/data/dka_data.db"
    patient_dao = PatientDataAccess(db_path)
    patients = patient_dao.get_all()
    patient_options = {p['hn']: p for p in patients}
    selected_hn = st.selectbox("Select Patient by HN", options=list(patient_options.keys()))

    if selected_hn:
        selected_patient = patient_options[selected_hn]
        st.write(f"**Name:** {selected_patient['name']}")
        st.write(f"**Age:** {selected_patient['age']}")
        st.write(f"**Sex:** {selected_patient['sex']}")

        # Display lab results for the selected patient
        st.subheader("Lab Results")
        lab_dao = LabDataAccess(db_path)
        lab_results = lab_dao.get_all_by_patient_id(selected_patient['patient_id'])
        if lab_results:
            st.table(lab_results)
        else:
            st.write("No lab results available for this patient.")

        # Add new lab result
        st.subheader("Add New Lab Result")
        dtx = st.number_input("Dextrostix (mg/dL)", step=1.0, value=st.session_state.get("dtx", 0.0))
        ph = st.number_input("pH Level", step=0.01, value=st.session_state.get("ph", 7.0))
        k = st.number_input("Potassium Level (mmol/L)", step=0.01, value=st.session_state.get("k", 0.0))
        na = st.number_input("Sodium Level (mmol/L)", step=0.01, value=st.session_state.get("na", 0.0))
        
        col1, col2 = st.columns(2)
        with col1:
            sampled_date = st.date_input("Sampled Date", value=st.session_state.get("sampled_date", datetime.date.today()))
        with col2:
            sampled_time = st.time_input("Sampled Time", value=st.session_state.get("sampled_time", datetime.time(0, 0)))

        col3, col4 = st.columns(2)
        with col3:
            result_date = st.date_input("Result Date", value=st.session_state.get("result_date", datetime.date.today()))
        with col4:
            result_time = st.time_input("Result Time", value=st.session_state.get("result_time", datetime.time(0, 0)))

        if st.button("Add Lab Result"):
            try:
                lab_data = {
                    "patient_id": selected_patient['patient_id'],
                    "logtime": int(time.time() * 1000),
                    "dtx": dtx,
                    "ph": ph,
                    "k": k,
                    "na": na,
                    "sampled_time": int(datetime.datetime.combine(sampled_date, sampled_time).timestamp() * 1000),
                    "result_time": int(datetime.datetime.combine(result_date, result_time).timestamp() * 1000),
                }
                lab_dao.insert(lab_data)
                st.success("Lab result added successfully!")
            except Exception as e:
                st.error(f"Error: {e}")

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