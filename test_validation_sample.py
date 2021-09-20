import pandas as pd
import pipeline.cohort_builder as cb
import pipeline.annotator as an
import pipeline.risk_scorer as rs
import pipeline.analyzer as al
import pipeline.config as config
from sklearn.metrics import classification_report, confusion_matrix, multilabel_confusion_matrix, cohen_kappa_score
from datetime import date
today = date.today()
today_str = today.strftime("%d_%m_%Y")

print("Load helper functions")

def filter_doc_id(x):
    if x in doc_ids:
        return True
    else:
        return False

def filter_doc_id_nlp(x):
    if x in doc_ids_nlp:
        return True
    else:
        return False
    
def convert_to_binary(x):
    if x == "YES":
        return 1
    else:
        return 0

print("Load validation samples")
ann1_filepath = "/home/jovyan/notebooks/alex/atrial_fibrillation/af_pipeline/output/AF_validation_template_20082021_AS_input_clean.csv"
ann1 = pd.read_csv(ann1_filepath)

ann2_filepath = "/home/jovyan/notebooks/alex/atrial_fibrillation/af_pipeline/output/AF_validation_template_20082021_YC_clean.csv"
ann2 = pd.read_csv(ann2_filepath)

raw_cohort_filepath = "/home/jovyan/notebooks/alex/atrial_fibrillation/af_pipeline/output/cohort_raw_table_18082021.csv"
raw_cohort_sum = pd.read_csv(raw_cohort_filepath)

doc_ids = ann1["Document Id"].unique()

nlp = raw_cohort_sum[raw_cohort_sum["doc_id"].apply(filter_doc_id)]

print("Align validation samples")
nlp_keep = ["doc_id", "af_diagnosis", "apixaban", "edoxaban", "rivaroxaban", "dabigatran", "warfarin", 
          "aspirin", "clopidogrel", "dipyridamole", "prasugrel", "ticagrelor",
         "congestive_heart_failure_chadsvasc", "hypertension_chadsvasc", "stroke_chadsvasc", "vascular_disease_chadsvasc", "diabetes_chadsvasc"]

nlp["af_diagnosis"] = 1 

nlp_join = nlp[nlp_keep]

#remove ids which are in annotation sample but not in nlp sample 
#(because annotation sample sourced from cohort builder module prior to demographic filter in annotator module)

doc_ids_nlp = nlp["doc_id"].unique()
ann1 = ann1[ann1["Document Id"].apply(filter_doc_id_nlp)]

ann1.columns = nlp_join.columns
ann1 = ann1.sort_values(by="doc_id")

ann2 = ann2[ann2["Document Id"].apply(filter_doc_id_nlp)]

ann2.columns = nlp_join.columns
ann2 = ann2.sort_values(by="doc_id")

#NOTE - will need to add code to create comparison and "gold standard" between ann1 and ann2 

for col in ann1.columns:
    if col != "doc_id":
        ann1[col] = ann1[col].apply(convert_to_binary)
        
for col in ann2.columns:
    if col != "doc_id":
        ann2[col] = ann2[col].apply(convert_to_binary)
        
print("Create inter annotator agreement table for each component")

ia_table = []

for col in ann1.columns:
    ia_entry = {}
    if col != "doc_id":
        print(col)
        ia_entry["component"] = col
        ann1_a = ann1[col].values
        ann2_a = ann2[col].values
        
        ck_score = cohen_kappa_score(ann1_a, ann2_a)
        ia_entry["cohen kappa"] = ck_score
        
        ia_table.append(ia_entry)
        
ia_table_df = pd.DataFrame(ia_table)
print("ia output table", ia_table_df)

ia_filepath = "./output/af_validation_ia_comparison_" + today_str + ".csv"
ia_table_df.to_csv(ia_filepath, index=False)

print("Create gold standard")

gs_ann = ann1.merge(ann2, on="doc_id")

def create_gs(x, col):
    col_x = col + "_x"
    col_y = col + "_y"
    
    if (x[col_x] == 1) & (x[col_y] == 1):
        return 1
    elif (x[col_x] == 1) & (x[col_y] == 0):
        return 1
    elif (x[col_x] == 0) & (x[col_y] == 1):
        return 1
    else:
        return 0
    
for col in ann1.columns:
    if col != "doc_id":
        gs_ann[col] = gs_ann.apply(create_gs, args=(col, ), axis=1)

gs_ann = gs_ann[nlp_keep]
        
print("Create comparison table for each component vs gold standard")

gs_table = []

for col in gs_ann.columns:
    gs_entry = {}
    if col != "doc_id":
        print(col)
        gs_entry["component"] = col
        y_true = gs_ann[col].values
        y_pred = nlp[col].values
        
        if (sum(y_true) == 0) and (sum(y_pred) == 0):
            print("Both gold standard and prediction predicted all zeroes, no confusion matrix possible")
            gs_entry["accuracy"] = 0
            gs_entry["precision"] = 0
            gs_entry["recall"] = 0
            gs_entry["p"] = 0
            gs_entry["tp"] = 0
            gs_entry["tn"] = 0
            gs_entry["fp"] = 0
            gs_entry["fn"] = 0
        else:  
            tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
            accuracy = (tp + tn ) / (tn + fp + fn + tp)
            precision = tp / (tp + fp)
            recall = tp / (tp + fn)
            gs_entry["accuracy"] = accuracy
            gs_entry["precision"] = precision
            gs_entry["recall"] = recall
            gs_entry["p"] = tp + fn
            gs_entry["tp"] = tp
            gs_entry["tn"] = tn
            gs_entry["fp"] = fp
            gs_entry["fn"] = fn
            
        
        gs_table.append(gs_entry)
        
gs_table_df = pd.DataFrame(gs_table)
print("gs output table", gs_table_df)

gs_filepath = "./output/af_validation_gs_comparison_" + today_str + ".csv"
gs_table_df.to_csv(gs_filepath, index=False)

