import streamlit as st
from src.logic.lab_data_access import LabDataAccess

def add_lab_result():
    st.header("Add Lab Result")
    patient_id = st.number_input("Patient ID", min_value=1, step=1, value=st.session_state.get("patient_id", 1))
    logtime = st.text_input("Log Time (Unix Timestamp in ms)", value=st.session_state.get("logtime", ""))
    sampled_time = st.text_input("Sampled Time (Unix Timestamp in ms)", value=st.session_state.get("sampled_time", ""))
    result_time = st.text_input("Result Time (Unix Timestamp in ms)", value=st.session_state.get("result_time", ""))
    dtx = st.number_input("Dextrostix (mg/dL)", step=1.0, value=st.session_state.get("dtx", 0.0))
    ph = st.number_input("pH Level", step=0.01, value=st.session_state.get("ph", 7.0))
    k = st.number_input("Potassium Level (mmol/L)", step=0.01, value=st.session_state.get("k", 0.0))
    na = st.number_input("Sodium Level (mmol/L)", step=0.01, value=st.session_state.get("na", 0.0))
    ag = st.number_input("Anion Gap (mmol/L)", step=0.01, value=st.session_state.get("ag", 0.0))
    ketone = st.number_input("Ketone (mmol/L)", step=0.01, value=st.session_state.get("ketone", 0.0))

    # Save inputs to session state
    st.session_state["patient_id"] = patient_id
    st.session_state["logtime"] = logtime
    st.session_state["sampled_time"] = sampled_time
    st.session_state["result_time"] = result_time
    st.session_state["dtx"] = dtx
    st.session_state["ph"] = ph
    st.session_state["k"] = k
    st.session_state["na"] = na
    st.session_state["ag"] = ag
    st.session_state["ketone"] = ketone

    if st.button("Add Lab Result"):
        try:
            lab_data = {
                "patient_id": patient_id,
                "logtime": int(logtime),
                "sampled_time": int(sampled_time),
                "result_time": int(result_time),
                "dtx": dtx,
                "ph": ph,
                "k": k,
                "na": na,
                "ag": ag,
                "ketone": ketone,
            }
            db_path = "./src/data/dka_data.db"
            lab_dao = LabDataAccess(db_path)
            lab_dao.insert(lab_data)
            st.success("Lab result added successfully!")
        except Exception as e:
            st.error(f"Error: {e}")