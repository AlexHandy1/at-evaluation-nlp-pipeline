import pandas as pd
import pipeline.cohort_builder as cb
import pipeline.annotator as an
import pipeline.risk_scorer as rs
import pipeline.analyzer as al
import pipeline.config as config

builder = cb.CohortBuilder()
annotator = an.Annotator()
risk_scorer = rs.RiskScorer()
analyzer = al.Analyzer()

#create cohort
search_term = "atrial fibrillation"
es_index_name = "ads_letters"
batch_size = 10000
cohort = builder.build_cohort(search_term, es_index_name, batch_size, trust_site = "UCLH")

cohort = cohort.reset_index().iloc[:, 1:]

#create random sample of 50 documents
#(v13072021)
sample = cohort.sample(n=50, random_state=2)
print("Sample", sample)

#prepare format for medcat trainer
sample_med = sample[["clinicalnotekey", "notetext"]]
sample_med.columns = ["name", "text"]
print("Sample for medcat", sample_med)

filepath = "./output/af_sample_docs_12082021_test.csv"
sample_med.to_csv(filepath, index=False)