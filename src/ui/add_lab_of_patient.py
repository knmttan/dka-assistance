import streamlit as st
import time
import datetime
from src.logic.patient_data_access import PatientDataAccess
from src.logic.lab_data_access import LabDataAccess
import pandas as pd

def patient_and_lab_details():
    st.header("Patient and Lab Details")

    # Select or add a patient
    st.subheader("Select Patient")
    db_path = "./src/data/dka_data.db"
    patient_dao = PatientDataAccess(db_path)
    patients = patient_dao.get_all()
    patient_options = {f" {p['name']} ({p['hn']})": p for p in patients}
    selected_hn = st.selectbox("Select Patient by HN and Name", options=list(patient_options.keys()))

    if selected_hn:
        selected_patient = patient_options[selected_hn]
        st.write(f"**Name:** {selected_patient['name']}")
        st.write(f"**Age:** {selected_patient['age']}")
        st.write(f"**Sex:** {selected_patient['sex']}")

        # Display lab results for the selected patient
        st.subheader("Lab Results")
        lab_dao = LabDataAccess(db_path)
        
        lab_results_raw = lab_dao.get_all_by_patient_id(selected_patient['patient_id'])
        print(lab_results_raw[0].keys())
        lab_results = pd.DataFrame(lab_results_raw, columns=lab_results_raw[0].keys())
        print(lab_results)
        lab_results = lab_results[["sampled_time", "result_time", "dtx", "ph", "k", "na", "ag", "ketone"]]
        lab_results["sampled_time"] = pd.to_datetime(lab_results["sampled_time"], unit='ms')
        lab_results["result_time"] = pd.to_datetime(lab_results["result_time"], unit='ms')
        lab_results["sampled_time"] = lab_results["sampled_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
        lab_results["result_time"] = lab_results["result_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
        lab_results["dtx"] = lab_results["dtx"].astype(float).round(2)
        lab_results["ph"] = lab_results["ph"].astype(float).round(2)
        lab_results["k"] = lab_results["k"].astype(float).round(2)
        lab_results["na"] = lab_results["na"].astype(float).round(2)
        lab_results["ag"] = lab_results["ag"].astype(float).round(2)
        lab_results["ketone"] = lab_results["ketone"].astype(float).round(2)
        lab_results = lab_results.rename(columns={"sampled_time": "Sampled Time", "result_time": "Result Time"})
        lab_results = lab_results.rename(columns={"dtx": "DTX (mg/dL)", "ph": "pH", "k": "K (mmol/L)", "na": "Na (mmol/L)", "ag": "AG (Anion Gap)", "ketone": "Ketone (mmol/L)"})
        lab_results = lab_results.sort_values(by="Sampled Time", ascending=True)
    
        if lab_results is not None and not lab_results.empty:
            st.dataframe(lab_results, hide_index=True)
        else:
            st.write("No lab results available for this patient.")

        # Add new lab result
        st.subheader("Add New Lab Result")
        dtx = st.number_input("Dextrostix (mg/dL)", step=1.0, value=st.session_state.get("dtx", 0.0))
        ph = st.number_input("pH Level", step=0.01, value=st.session_state.get("ph", 7.0))
        k = st.number_input("Potassium Level (mmol/L)", step=0.01, value=st.session_state.get("k", 0.0))
        na = st.number_input("Sodium Level (mmol/L)", step=0.01, value=st.session_state.get("na", 0.0))
        ag = st.number_input("Anion Gap (mmol/L)", step=0.01, value=st.session_state.get("ag", 0.0))
        ketone = st.number_input("Ketone (mmol/L)", step=0.01, value=st.session_state.get("ketone", 0.0))
        
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
                    "ag": ag,
                    "ketone": ketone,
                    "sampled_time": int(datetime.datetime.combine(sampled_date, sampled_time).timestamp() * 1000),
                    "result_time": int(datetime.datetime.combine(result_date, result_time).timestamp() * 1000),
                }
                lab_dao.insert(lab_data)
                st.success("Lab result added successfully!")
            except Exception as e:
                st.error(f"Error: {e}")