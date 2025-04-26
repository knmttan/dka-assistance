class LabResult:
    def __init__(self, lab_dao, patient_id=None, lab_results_raw=None):
        self.lab_dao = lab_dao
        self.patient_id = patient_id
        self.lab_results_raw = lab_results_raw or (self.lab_dao.get_all_by_patient_id(patient_id) if patient_id else None)

    def format_and_output(self):
        import pandas as pd
        if self.lab_results_raw is not None and len(self.lab_results_raw) > 0:
            lab_results = pd.DataFrame(self.lab_results_raw, columns=self.lab_results_raw[0].keys())
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
            return lab_results
        else:
            return None

    def add_lab_result(self, lab_data):
        error_list = LabResult.validate_input(
            lab_data.get("dtx"),
            lab_data.get("ph"),
            lab_data.get("k"),
            lab_data.get("na"),
            lab_data.get("ag"),
            lab_data.get("ketone"),
            lab_data.get("sampled_time"),
            lab_data.get("result_time")
        )
        if error_list:
            raise ValueError(f"Validation errors: {error_list}")

        self.lab_dao.insert(lab_data)

    @staticmethod
    def validate_input(dtx, ph, k, na, ag, ketone, sampled_date, result_date):
        errors = []
        if not sampled_date:
            errors.append("Sampled date is required.")
        if not result_date:
            errors.append("Result date is required.")
        if dtx is not None and dtx < 0:
            errors.append("Dextrostix cannot be negative.")
        if ph is not None and (ph < 0 or ph > 14):
            errors.append("pH level must be between 0 and 14.")
        if k is not None and k < 0:
            errors.append("Potassium level cannot be negative.")
        if na is not None and na < 0:
            errors.append("Sodium level cannot be negative.")
        if ag is not None and ag < 0:
            errors.append("Anion gap cannot be negative.")
        if ketone is not None and ketone < 0:
            errors.append("Ketone level cannot be negative.")
        return errors