import streamlit as st
import time
import datetime
from src.logic.treatment_data_access import TreatmentDataAccess
from src.logic.patient_data_access import PatientDataAccess
from src.logic.lab_data_access import LabDataAccess

def add_treatment():
    st.header("Add Administered Treatment")
    patient_id = st.number_input("Patient ID", min_value=1, step=1, value=st.session_state.get("treatment_patient_id", 1))
    logtime = st.text_input("Log Time (Unix Timestamp in ms)", value=st.session_state.get("treatment_logtime", ""))

    col1, col2 = st.columns(2)
    with col1:
        administered_date = st.date_input("Administered Date", value=st.session_state.get("administered_date", datetime.date.today()))
    with col2:
        administered_time = st.time_input("Administered Time", value=st.session_state.get("administered_time", datetime.time(0, 0)))

    col3, col4 = st.columns(2)
    with col3:
        end_date = st.date_input("End Date", value=st.session_state.get("end_date", datetime.date.today()))
    with col4:
        end_time = st.time_input("End Time", value=st.session_state.get("end_time", datetime.time(0, 0)))

    treatment_id = st.number_input("Treatment ID", min_value=1, step=1, value=st.session_state.get("treatment_id", 1))
    application_method_id = st.number_input("Application Method ID", min_value=1, step=1, value=st.session_state.get("application_method_id", 1))
    administration_rate = st.number_input("Administration Rate", step=1.0, value=st.session_state.get("administration_rate", 0.0))

    # Save inputs to session state
    st.session_state["treatment_patient_id"] = patient_id
    st.session_state["treatment_logtime"] = logtime
    st.session_state["administered_date"] = administered_date
    st.session_state["administered_time"] = administered_time
    st.session_state["end_date"] = end_date
    st.session_state["end_time"] = end_time
    st.session_state["treatment_id"] = treatment_id
    st.session_state["application_method_id"] = application_method_id
    st.session_state["administration_rate"] = administration_rate

    if st.button("Add Treatment"):
        try:
            treatment_data = {
                "patient_id": patient_id,
                "logtime": int(logtime),
                "administored_time": int(datetime.datetime.combine(administered_date, administered_time).timestamp() * 1000),
                "end_time": int(datetime.datetime.combine(end_date, end_time).timestamp() * 1000),
                "treatment_id": treatment_id,
                "application_method_id": application_method_id,
                "administration_rate": administration_rate,
            }
            db_path = "./src/data/dka_data.db"
            treatment_dao = TreatmentDataAccess(db_path)
            treatment_dao.insert(treatment_data)
            st.success("Treatment added successfully!")
        except Exception as e:
            st.error(f"Error: {e}")

def suggest_treatment():
    st.header("Treatment Suggestions")

    # Select a patient
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

        # Fetch the latest lab result for the selected patient
        lab_dao = LabDataAccess(db_path)
        lab_results = lab_dao.get_all_by_patient_id(selected_patient['patient_id'])

        # Display lab results for the selected patient
        st.subheader("Lab Results")
        if lab_results:
            st.table(lab_results)
        else:
            st.write("No lab results available for this patient.")

        # Fetch the latest lab result for the selected patient
        if lab_results:
            latest_lab = lab_results[-1]  # Assuming the last entry is the latest
            st.subheader("Latest Lab Results")
            st.write(f"Dextrostix: {latest_lab['dtx']} mg/dL")
            st.write(f"pH Level: {latest_lab['ph']}")
            st.write(f"Potassium Level: {latest_lab['k']} mmol/L")
            st.write(f"Sodium Level: {latest_lab['na']} mmol/L")

            # Suggest treatment based on lab results
            st.subheader("Suggested Treatment")
            if latest_lab['dtx'] < 70:
                st.write("Administer glucose.")
            elif latest_lab['ph'] < 7.3:
                st.write("Administer bicarbonate.")
            else:
                st.write("No immediate treatment required.")

            # Input actual treatment administered
            st.subheader("Record Administered Treatment")
            treatment_id = st.text_input("Treatment ID")
            application_method = st.text_input("Application Method")
            administration_rate = st.number_input("Administration Rate", step=1.0)

            if st.button("Save Treatment"):
                try:
                    treatment_dao = TreatmentDataAccess(db_path)
                    treatment_data = {
                        "patient_id": selected_patient['patient_id'],
                        "treatment_id": treatment_id,
                        "application_method": application_method,
                        "administration_rate": administration_rate,
                        "logtime": int(time.time() * 1000),
                    }
                    treatment_dao.insert(treatment_data)
                    st.success("Treatment recorded successfully!")
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("No lab results available for this patient.")