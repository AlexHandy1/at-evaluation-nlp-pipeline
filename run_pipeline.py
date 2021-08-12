import time
from datetime import datetime
import pandas as pd
import logging
import psutil
import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')
from plotnine import ggplot, aes, geom_point, geom_pointrange, geom_hline, coord_flip, xlab, ylab, theme_bw, facet_grid, geom_text

current_time = datetime.now().strftime("%H:%M:%S")
logging_filename = "annotation" + current_time + ".log"
logging.basicConfig(filename=logging_filename, level=logging.DEBUG, format='%(asctime)s %(message)s', filemode="w")

start = time.time()
print("Start pipeline at: ", datetime.fromtimestamp(start))

memory_start = psutil.virtual_memory().available / (1024.0 ** 3)
print("GB memory available start: ", memory_start)

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

#annotate cohort
annotated_cohort = annotator.annotate_cohort(cohort, config.Config().es_config["non_es_demographics_path"])
print("Final cohort length:", len(annotated_cohort))

memory_post_annotation = psutil.virtual_memory().available / (1024.0 ** 3)
print("GB memory available at end of annotations: ", memory_post_annotation)

#add risk scores
definitions = [config.Config().codelists_config["chadsvasc_path"], config.Config().codelists_config["hasbled_path"]]
scores = risk_scorer.generate_cohort_scores(annotated_cohort, definitions)
print("Scored cohort shape", scores.shape)

#add medication data
med_scores = risk_scorer.generate_medication_flags(annotated_cohort, scores, config.Config().codelists_config["meds_path"])
print("Medications added to cohort shape", med_scores.shape)

#prep for analysis
cols_to_binary = config.Config().codelists_config["chadsvasc_components_2pts"] + config.Config().codelists_config["medications"]
cohort_df = analyzer.add_medication_categories(analyzer.convert_counts_to_binary_flags(med_scores, cols_to_binary))

#save cohort
print("Saving raw cohort table")
cohort_df.to_csv(config.Config().output_config["cohort_raw_table_filepath"])

#create summary table and save
print("Create and save cohort summary table")
splits = ['total', 'any_at', 'ac_only', 'ap_only', 'ac_and_ap', 'no_at']
cohort_summary = analyzer.build_summary_table(cohort_df, splits, config.Config().output_config["cohort_summary_table_filepath"])
print("Cohort summary", cohort_summary)

#create subset of cohort with CHA2DS2-VASc >=2 
med_scores_gtech2 = med_scores[med_scores["total_chadsvasc"]>=2]
print("Cohort CHA2DS2-VASc >=2 shape", med_scores_gtech2.shape)

#create prescribing trends plot and save
print("Create and save prescribing trends plot")
drug_categories = ["ac_only", "ac_and_ap", "ap_only", "no_at"]
drug_categories_clean = ["AC only", "AC and AP", "AP only", "No AT"]
fig, ax = plt.subplots(nrows=1, ncols=1)
fig.subplots_adjust(hspace=0.3, wspace=0.0)
fig.set_size_inches(11, 7)

ax = analyzer.plot_prescribing_trends_by_drug(med_scores_gtech2, drug_categories, ax, freq="Q", clean_labels = drug_categories_clean)
fig.savefig(config.Config().output_config["prescribing_trends_filepath"], dpi=300, bbox_inches='tight')

#run factor analysis and create / save plot 
print("Create and save prescribing trends plot")

med_scores_gtech2 = analyzer.normalize_factor(med_scores_gtech2, "age")
factors = ["age_z", "female", "hypertension_chadsvasc", "diabetes_chadsvasc", "congestive_heart_failure_chadsvasc", "vascular_disease_chadsvasc", "liver_disease_hasbled", "renal_disease_hasbled", "alcohol_hasbled", "stroke_hasbled"]
clean_factors = ["Age (z)", "Female", "Hypertension", "Diabetes", "Congestive heart failure", "Vascular disease", "Liver disease", "Renal disease", "Harmful alcohol use", "Stroke"]
reg_output = analyzer.run_regression(med_scores_gtech2, med_scores_gtech2["any_at"], factors)

reg_output_for_plot = reg_output.reset_index()
reg_output_for_plot.columns = ["factor", "ci_lower", "ci_upper", "odds_ratio", "raw_p", "significant", "p", "or_text"]
reg_output_for_plot["clean_factor"] = clean_factors
print("Check format for reg output for plot", reg_output_for_plot)

factor_list = reg_output_for_plot.sort_values(by="odds_ratio", ascending=False)["clean_factor"].tolist()
reg_output_for_plot["clean_factor_cat"] = pd.Categorical(reg_output_for_plot['clean_factor'], categories=factor_list)

#plot and save
factor_plot = ggplot(reg_output_for_plot) + aes(x="clean_factor_cat", y="odds_ratio", ymin="ci_lower", ymax="ci_upper") + geom_pointrange() + geom_text(label=round(reg_output_for_plot["odds_ratio"], 2), size=8, nudge_x=0.2) + geom_hline(yintercept=1, linetype='dotted', size=1) + coord_flip() + xlab("Factor") + ylab("Odds Ratio (95% CI)") + theme_bw()
factor_plot.save(config.Config().output_config["factor_plot_filepath"], dpi = 300)

memory_end = psutil.virtual_memory().available / (1024.0 ** 3)
print("GB memory available finish: ", memory_end)

end = time.time()
print("Finish pipeline at: ", datetime.fromtimestamp(start))
print("Completed in %s minutes" % ( round(end - start,2) / 60 ) )