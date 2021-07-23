#CONFIG NEEDS TO BE COMPLETED - DUMMY DATA BELOW
class Config():
    def __init__(self):
        self.es_config = {
            
            #for use in cohort builder module
            
            #gae02 (Internal server name)
            "gae02_user": "xxx",
            "gae02_password": "xxx",
            
            #gae03 (Internal server name)
            "gae03_ca_certs_path": "xxx",
            "gae03_hosts": "xxx",
            "gae03_port": "xxx",
            "gae03_user": "xxx",
            "gae03_password": "xxx",
            
            #csv with date of birth and gender that could not be ingested into cogstack due to ethics
            "non_es_demographics_path": "./pipeline/cohort_metadata/xxx.csv"
            
        }
        
        self.medcat_config = {
            #for use in annotation module
            "cdb_path": "./pipeline/annotation_models/xxx.dat",
            "vocab_path": "./pipeline/annotation_models/xxx.dat",
            "meta_path": "./pipeline/annotation_models/xxx/"
        }
        
        self.codelists_config = {
            #for use in risk scoring module
            "chadsvasc_path": "./pipeline/risk_score_definition/22_07_2021_chads.csv",
            "hasbled_path": "./pipeline/risk_score_definition/22_07_2021_hasbled.csv",
            "meds_path": "./pipeline/risk_score_definition/22_07_2021_meds.csv",
            
            #for use in analysis module
            "medications": ['warfarin', 'aspirin', 'apixaban', 'prasugrel','clopidogrel', 'dipyridamole', 'rivaroxaban', 'ticagrelor','dabigatran', 'edoxaban'],
            "chadsvasc_components": ['age_65_74_chadsvasc', 'age_gte75_chadsvasc','female_chadsvasc','congestive_heart_failure_chadsvasc', 'diabetes_chadsvasc', 'hypertension_chadsvasc', 'stroke_chadsvasc', 'vascular_disease_chadsvasc'],
            "chadsvasc_components_clean_labels": ['Age 65-74', 'Age >=75','Female','Congestive heart failure', 'Diabetes', 'Hypertension', 'Stroke / TIA / thromboembolism', 'Vascular disease'],
            "chadsvasc_components_2pts":['age_65_74_chadsvasc', 'age_gte75_chadsvasc', 'stroke_chadsvasc'],
            "hasbled_components": ['age_gt65_hasbled', 'liver_disease_hasbled', 'renal_disease_hasbled', 'stroke_hasbled', "bleeding_hasbled", 'bleeding_medications_hasbled', 'alcohol_hasbled'],
            "hasbled_components_clean_labels": ['Age >65', 'Liver disease', 'Renal disease', 'Stroke', "Major bleeding event", 'Bleeding medication', 'Harmful alcohol use']
            
        }
        
        self.output_config = {
            "cohort_raw_table_filepath": "cohort_raw_table_xxx.csv",
            "cohort_summary_table_filepath": "cohort_summary_table_xxx.csv",
            "prescribing_trends_filepath": "prescribing_trends_xxx.png",
            "factor_plot_filepath": "factor_plot_xxx.png"
        }
        