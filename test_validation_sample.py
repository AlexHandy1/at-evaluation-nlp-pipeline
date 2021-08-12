import pandas as pd
import pipeline.cohort_builder as cb
import pipeline.annotator as an
import pipeline.risk_scorer as rs
import pipeline.analyzer as al
import pipeline.config as config
from sklearn.metrics import classification_report, confusion_matrix, multilabel_confusion_matrix

print("Load helper functions")

def filter_doc_id(x):
    if x in doc_ids:
        return True
    else:
        return False
    
def convert_to_binary(x):
    if x == "YES":
        return 1
    else:
        return 0

print("Load validation samples")
ann1_filepath = "/home/jovyan/notebooks/alex/atrial_fibrillation/af_pipeline/output/AF_validation_template_13072021_AS_input_10082021.csv"
ann1 = pd.read_csv(ann1_filepath)

raw_cohort_filepath = "/home/jovyan/notebooks/alex/atrial_fibrillation/af_pipeline/output/cohort_raw_table_22072021.csv"
raw_cohort_sum = pd.read_csv(raw_cohort_filepath)

doc_ids = ann1["Document Id"].unique()

nlp = raw_cohort_sum[raw_cohort_sum["doc_id"].apply(filter_doc_id)]

print("Align validation samples")
nlp_keep = ["doc_id", "af_diagnosis", "apixaban", "edoxaban", "rivaroxaban", "dabigatran", "warfarin", 
          "aspirin", "clopidogrel", "dipyridamole", "prasugrel", "ticagrelor",
         "congestive_heart_failure_chadsvasc", "hypertension_chadsvasc", "stroke_chadsvasc", "vascular_disease_chadsvasc", "diabetes_chadsvasc"]

nlp["af_diagnosis"] = 1 

nlp_join = nlp[nlp_keep]

ann1.columns = nlp_join.columns
ann1 = ann1.sort_values(by="doc_id")

#NOTE - will need to add code to create comparison and "gold standard" between ann1 and ann2 

for col in ann1.columns:
    if col != "doc_id":
        ann1[col] = ann1[col].apply(convert_to_binary)
        
print("Create comparison table for each component vs gold standard")

table = []

for col in ann1.columns:
    entry = {}
    if col != "doc_id":
        print(col)
        entry["component"] = col
        y_true = ann1[col].values
        y_pred = nlp[col].values
        
        if (sum(y_true) == 0) and (sum(y_pred) == 0):
            print("Both gold standard and prediction predicted all zeroes, no confusion matrix possible")
            entry["accuracy"] = 0
            entry["precision"] = 0
            entry["recall"] = 0
            entry["p"] = 0
            entry["tp"] = 0
            entry["tn"] = 0
            entry["fp"] = 0
            entry["fn"] = 0
        else:  
            tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
            accuracy = (tp + tn ) / (tn + fp + fn + tp)
            precision = tp / (tp + fp)
            recall = tp / (tp + fn)
            entry["accuracy"] = accuracy
            entry["precision"] = precision
            entry["recall"] = recall
            entry["p"] = tp + fn
            entry["tp"] = tp
            entry["tn"] = tn
            entry["fp"] = fp
            entry["fn"] = fn
            
        
        table.append(entry)
        
table_df = pd.DataFrame(table)
print("output table", table_df)

filepath = "./output/af_validation_comparison_120821_test.csv"
table_df.to_csv(filepath, index=False)