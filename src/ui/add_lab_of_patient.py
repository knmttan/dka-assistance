import streamlit as st
import time
import datetime
from src.logic.patient_data_access import PatientDataAccess
from src.logic.lab_data_access import LabDataAccess
from src.logic.input_output_utils import LabResult

def patient_and_lab_details():
    st.header("Patient and Lab Details")

    # SECTION - Patient data
    # Select or add a patient
    st.subheader("Select Patient")
    db_path = "./src/data/dka_data.db"
    patient_dao = PatientDataAccess(db_path)
    patients = patient_dao.get_all()
    patient_options = {f" {p['name']} ({p['hn']})": p for p in patients}
    selected_hn = st.selectbox("Select Patient by HN and Name", options=list(patient_options.keys()))

    if not selected_hn:
        st.warning("Please select a patient.")
        return
    
    selected_patient = patient_options[selected_hn]
    st.write(f"**Name:** {selected_patient['name']}")
    st.write(f"**Age:** {selected_patient['age']}")
    st.write(f"**Sex:** {selected_patient['sex']}")

    # Display lab results for the selected patient
    st.subheader("Lab Results")
    lab_dao = LabDataAccess(db_path)
    lab_result = LabResult(lab_dao, patient_id=selected_patient['patient_id'])
    formatted_lab_results = lab_result.format_and_output()

    if formatted_lab_results is not None:
        st.dataframe(formatted_lab_results, hide_index=True)
    else:
        st.write("No lab results available for this patient.")

    # Add new lab result
    st.subheader("Add New Lab Result")

    # Adjust the width of date_input and time_input using Streamlit's style settings
    st.write("**Sampled Date Time**")
    col1, col2 = st.columns(2)
    with col1:
        sampled_date = st.date_input("Sampled Date", value=st.session_state.get("sampled_date", datetime.date.today()))
    with col2:
        sampled_time = st.time_input("Sampled Time", value=st.session_state.get("sampled_time", datetime.datetime.now().replace(minute=0, second=0, microsecond=0).time()))
    st.divider()
    st.write("**Result Date Time**")
    col3, col4 = st.columns(2)
    with col3:
        result_date = st.date_input("Result Date", value=st.session_state.get("result_date", datetime.date.today()))
    with col4:
        result_time = st.time_input("Result Time", value=st.session_state.get("result_time", datetime.datetime.now().replace(minute=0, second=0, microsecond=0).time()))
    st.divider()
    st.write("**Lab Result Values**")
    dtx = st.number_input("Dextrostix (mg/dL)", step=1.0, value=st.session_state.get("dtx", None), format="%.2f")
    ph = st.number_input("pH Level", step=0.01, value=st.session_state.get("ph", None), format="%.2f")
    k = st.number_input("Potassium Level (mmol/L)", step=0.01, value=st.session_state.get("k", None), format="%.2f")
    na = st.number_input("Sodium Level (mmol/L)", step=0.01, value=st.session_state.get("na", None), format="%.2f")
    ag = st.number_input("Anion Gap (mmol/L)", step=0.01, value=st.session_state.get("ag", None), format="%.2f")
    ketone = st.number_input("Ketone (mmol/L)", step=0.01, value=st.session_state.get("ketone", None), format="%.2f")

    # Validate inputs
    errors = LabResult.validate_input(dtx, ph, k, na, ag, ketone, sampled_date, result_date)
    if errors:
        for error in errors:
            st.error(error)
    else:
        if st.button("Add Lab Result"):
            @st.dialog("Review Lab Result", width="large")
            def review_and_add_lab_result():
                st.write("**Dextrostix (mg/dL):**", dtx)
                st.write("**pH Level:**", ph)
                st.write("**Potassium Level (mmol/L):**", k)
                st.write("**Sodium Level (mmol/L):**", na)
                st.write("**Anion Gap (mmol/L):**", ag)
                st.write("**Ketone (mmol/L):**", ketone)
                st.write("**Sampled Date and Time:**", sampled_date, sampled_time)
                st.write("**Result Date and Time:**", result_date, result_time)

                if st.button("Confirm and Add"):
                    lab_data = {
                        "patient_id": selected_patient['patient_id'],
                        "logtime": int(time.time() * 1000),
                        "dtx": dtx,
                        "ph": ph,
                        "k": k,
                        "na": na,
                        "ag": ag,
                        "ketone": ketone,
                        "sampled_time": int(datetime.datetime.combine(sampled_date, sampled_time).timestamp() * 1000),
                        "result_time": int(datetime.datetime.combine(result_date, result_time).timestamp() * 1000),
                    }
                    try:
                        lab_result.add_lab_result(lab_data)
                        st.success("Lab result added successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

            review_and_add_lab_result()