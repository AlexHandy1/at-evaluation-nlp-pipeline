import pandas as pd
import time
from datetime import datetime
from random import sample
import statsmodels.api as sm
import numpy as np
import pipeline.config as config
from scipy.stats import chi2_contingency, kruskal

class Analyzer:
    def __init__(self):
        print("Initializing Analzyer")
        
        self.medications = config.Config().codelists_config["medications"]
        self.chadsvasc_risk_components = config.Config().codelists_config["chadsvasc_components"]
        self.chadsvasc_risk_components_clean_labels = config.Config().codelists_config["chadsvasc_components_clean_labels"]
        self.hasbled_risk_components = config.Config().codelists_config["hasbled_components"]
        self.hasbled_risk_components_clean_labels = config.Config().codelists_config["hasbled_components_clean_labels"]
    
    def convert_counts(self, x):
        '''
        Given a column with count data, return a binary flag indicating presence
        x: row with numeric data
        '''
        if x > 0:
            return 1
        else:
            return 0
        
    def convert_counts_to_binary_flags(self, cohort, cols):
        '''
        For a given cohort and list of cols, convert counts to binary flags
        cohort: structured pandas dataframe of risk scores and covariates to be processed for analysis
        cols: list of target cols with count metrics to be converted to binary flags
        '''
        for col in cols:
            cohort[col] = cohort[col].apply(self.convert_counts)
            
        return cohort
    
    def add_category_flag(self, x):
        '''
        Given the result of an "any" statement on row from an set of column variables, return a binary flag indicating presence
        x: row with bool data
        '''
        if x == True:
            return 1
        else:
            return 0
    
    def add_ap_only_flag(self, x):
        '''
        Given a medication category, return target binary indicator
        x: row with numeric data for medication category
        '''
        if (x["ac"] == 0) & (x["ap"] == 1):
            return 1
        else:
            return 0
    
    def add_ac_only_flag(self, x):
        '''
        Given a medication category, return target binary indicator
        x: row with numeric data for medication category
        '''
        if (x["ac"] == 1) & (x["ap"] == 0):
            return 1
        else:
            return 0
        
    def add_no_ac_flag(self, x):
        '''
        Given a medication category, return target binary indicator
        x: row with numeric data for medication category
        '''
        if x == 0:
            return 1
        else:
            return 0
    
    def add_medication_categories(self, cohort):
        '''
        Given a cohort as a pandas dataframe apply medication category labels and return cohort
        cohort: pandas dataframe for target cohort
        '''
               
        cohort["any_at"] = cohort[self.medications].any(axis=1).apply(self.add_category_flag)
        print("Any AT (AC or AP) %:", (cohort["any_at"].sum() / len(cohort)) * 100)
        
        cohort["no_at"] = cohort["any_at"].apply(self.add_no_ac_flag)
        print("No AT %:", (cohort["no_at"].sum() / len(cohort)) * 100)
        
        print("Any AT + No AT == all cohort")
        print((cohort["any_at"].sum() + cohort["no_at"].sum()) == len(cohort))

        cohort["doac"] = cohort[['apixaban','rivaroxaban','dabigatran','edoxaban']].any(axis=1).apply(self.add_category_flag)
        print("Any DOAC %: ", (cohort["doac"].sum() / len(cohort)) * 100)

        cohort["ac"] = cohort[['warfarin','doac']].any(axis=1).apply(self.add_category_flag)
        print("Any AC (warfarin + DOACs) %: ", (cohort["ac"].sum() / len(cohort)) * 100)

        cohort["ap"] = cohort[['aspirin','prasugrel', 'clopidogrel', 'dipyridamole', 'ticagrelor']].any(axis=1).apply(self.add_category_flag)
        print("Any AP %: ", (cohort["ap"].sum() / len(cohort)) * 100)

        cohort["ac_and_ap"] = cohort[['ap', 'ac']].all(axis=1).apply(self.add_category_flag)
        print("AC and AP %: ", (cohort["ac_and_ap"].sum() / len(cohort)) * 100)

        cohort["ap_only"] = cohort.apply(self.add_ap_only_flag, axis=1)
        print("AP only %: ", (cohort["ap_only"].sum() / len(cohort)) * 100)

        cohort["ac_only"] = cohort.apply(self.add_ac_only_flag, axis=1)
        print("AC only %: ", (cohort["ac_only"].sum() / len(cohort)) * 100)
        
        print("AC only + AP only + AC and AP == Any drug")
        print((cohort["ac_only"].sum() + cohort["ap_only"].sum() + cohort["ac_and_ap"].sum()) == cohort["any_at"].sum())
        
        return cohort
    
    def run_chi2_analysis(self, cohort, summary_table, test_cols, cat_vars):
        '''
        For target cohort run chi2 analysis on categorical variables and return in summary table
        cohort: pandas dataframe for target cohort
        summary_table: pandas dataframe of summary cohort created with build_summary_table
        test_cols: list of columns names for categories testing differences across e.g. medication categories
        cat_cars: list of categorical variables to test across medication categories
        '''
        
        for var in cat_vars:
            gr = cohort.groupby(var)
            table = gr[test_cols].sum()
            #print(table)
            chi2, p, dof, ex = chi2_contingency(table)
            print(var, ":", p)
            summary_table.loc[var, "p_value"] = p
            
        return summary_table
    
    def run_kruskal_analysis(self, cohort, summary_table, test_cols, cont_vars):
        '''
        For target cohort run kruskal analysis on continuous variables and return in summary table
        cohort: pandas dataframe for target cohort
        summary_table: pandas dataframe of summary cohort created with build_summary_table
        test_cols: list of columns names for categories testing differences across e.g. medication categories
        cont_cars: list of continuous variables to test across medication categories
        '''
        
        tests = {}
        for col in test_cols:
            d = cohort[cohort[col] == 1]
            tests[col] = {}

            for var in cont_vars:
                tests[col][var] = d[var].tolist() #get data on this factor for patients in this split group

        for var in cont_vars:
            stat = kruskal(tests['ac_only'][var], tests['ap_only'][var], tests['ac_and_ap'][var], tests['no_at'][var])
            p = stat.pvalue
            print(var, ":", p)
            summary_table.loc[var, "p_value"] = p
        
        return summary_table
        
    
    def build_summary_table(self, cohort, splits, cohort_summary_filepath):
        '''
        For target cohort generate, save and return a summary table
        cohort: pandas dataframe for target cohort
        splits: list of categories to present stratifications for
        cohort_summary_filepath: filepath to save csv of summary table
        '''
        index_names = ["Individuals", "age", "female"] + self.chadsvasc_risk_components + ["total_chadsvasc"] + [ "CHA2DS2-VASc " + str(i) for i in range(10) ]  + self.hasbled_risk_components + ["total_hasbled"] + [ "HAS-BLED " + str(i) for i in range(10) ]
        index_names_clean = ["Individuals", "Age (y)", "Female"] + self.chadsvasc_risk_components_clean_labels + ["CHA2DS2-VASc score"] + [ "CHA2DS2-VASc " + str(i) for i in range(10) ]  + self.hasbled_risk_components_clean_labels + ["HAS-BLED score"] + [ "HAS-BLED " + str(i) for i in range(10) ]
       
        dataframe_dict = {}
        for split in splits:
            entry = []
            if split == "total":
                df = cohort
            else:
                df = cohort[cohort[split] == 1]

            #individuals (n)
            entry.append(len(df)) 

            #mean age and sd
            entry.append(str(df["age"].mean().round(2)) + " +/- " + str(df["age"].std().round(2)))
                        
            #count and %female
            entry.append(str(df["female"].sum()) + " (" + str((df["female"].sum() / len(df) * 100).round(2)) + "%)")
            
            #% for each chadsvasc risk component
            for comp in self.chadsvasc_risk_components:
                entry.append( str(df[comp].sum()) + " (" + str((df[comp].sum() / len(df) * 100).round(2)) + "%)")
            
            #get mean chadsvasc score and sd
            entry.append(str(df["total_chadsvasc"].mean().round(2)) + " +/- " + str(df["total_chadsvasc"].std().round(2)))
            
            #count and % for each chadsvasc score number
            for i in range(10):
                try:
                    entry.append( str(df.groupby("total_chadsvasc").count()["doc_id"].values[i]) + " (" + str((df.groupby("total_chadsvasc").count()["doc_id"].values[i] / len(df) * 100).round(2)) + "%)")
                except:
                    entry.append( str(0) + " (" + str(0) + "%)")
            
            #% for each hasbled risk component
            for comp in self.hasbled_risk_components:
                entry.append( str(df[comp].sum()) + " (" + str((df[comp].sum() / len(df) * 100).round(2)) + "%)")
            
            #get mean hasbled score and sd
            entry.append(str(df["total_hasbled"].mean().round(2)) + " +/- " + str(df["total_hasbled"].std().round(2)))
            
            #count and % for each hasbled score number
            for i in range(10):
                try:
                    entry.append( str(df.groupby("total_hasbled").count()["doc_id"].values[i]) + " (" + str((df.groupby("total_hasbled").count()["doc_id"].values[i] / len(df) * 100).round(2)) + "%)")
                except:
                    entry.append( str(0) + " (" + str(0) + "%)")


            dataframe_dict[split] = entry

        summary_table = pd.DataFrame(dataframe_dict)
        summary_table.index = index_names
        
        #setup categorical difference analysis
        summary_table["p_value"] = 0
        cat_vars = ["female"] + config.Config().codelists_config["chadsvasc_components"] + config.Config().codelists_config["hasbled_components"]
        cont_vars = ["age", "total_chadsvasc", "total_hasbled"]
        test_cols = ["ac_only", "ap_only", "ac_and_ap", "no_at"]
        
        print("Run categorical difference analyses")
        summary_table = self.run_chi2_analysis(cohort, summary_table, test_cols, cat_vars)
        summary_table = self.run_kruskal_analysis(cohort, summary_table, test_cols, cont_vars)
        
        summary_table.index = index_names_clean
        summary_table.to_csv(cohort_summary_filepath)
        
        return summary_table
                
    def plot_prescribing_trends_by_drug(self, cohort, drugs, ax, date_col="date_stamp", freq="Q", clean_labels = None):
        '''
        For target cohort generate and return a prescribing trends chart
        cohort: pandas dataframe for target cohort
        drugs: names of drug columns to plot. Must be mutually exclusive columns 
               which gives the total number of patients when summed
        ax: axis for plot
        date_col: column name containing dates for aggregation
        freq: aggregation of dates, default 'M' (monthly), see 
                  pandas.DataFrame.resample for options
        clean_labels: list of labels for 'drugs' for use in legend
        '''
        #convert to pandas datetime and align to naming convention 'date_stamp'
        cohort["date_stamp"] = pd.to_datetime(cohort["encounter_date"])
        
        sum_cols = [date_col] + drugs

        #does not preserve every unique entry even though every timestamp is unique - why? (see if remains when only take one document per patient)
        per_day = cohort[sum_cols].groupby(date_col).sum() 
        per_day['date'] = per_day.index
        per_day = per_day.set_index('date')
        #drugs provided must sum to 100% - therefore, mutually exclusive categories (can't have an individual in multiple categories)
        per_day['total'] = per_day[drugs].sum(axis=1)

        #normalise for stacked plot - get the % of each drug as a proportion of the total drugs per day
        per_day_norm = per_day[drugs]
        for dr in drugs:
            norm = per_day[dr]/per_day['total']
            per_day_norm.loc[:,dr] = norm

        #resample average
        resampled = pd.DataFrame() 
        for dr in drugs:
            resampled[dr] = per_day_norm[dr].resample(freq).mean() * 100
        
        print("Summary table for visual review")
        print(resampled)

        resampled.loc[:,drugs].plot(kind='bar', stacked=True, width=1.1, ax=ax)
        t = resampled.index.tolist()
        tm = [x.strftime('%m-%Y') for x in t]
        v = ""
        for i in range(len(tm)):
            if tm[i] != v:
                v = tm[i]
            else:
                tm[i] = ""

        #formatting axes and legend
        ax.set_xticklabels(tm)
        ax.tick_params(axis='x', rotation=90)
        ax.set_xlabel('Date')
        ax.set_ylabel('Percent of individuals')
        patches, labels = ax.get_legend_handles_labels()
        
        if clean_labels:
            labels = clean_labels
            
        ax.legend(patches, labels, loc=2, bbox_to_anchor=(1.05, 1), frameon=True, ncol=1, facecolor='#FFFFFF')
        return ax
        
    #LEGACY function - note used in current pipeline but can generate prescribing trends chart by risk score   
    def plot_drugs_vs_score(self, cohort, drugs, ax, score_name, display_name="", panel=""):
        """
        Stacked plot of prescribing stratified by risk score with n above column.
        drugs: names of drug columns to plot. Must b emutually exclusive columns 
               which gives the total number of patients when summed
        ax: axis for plot
        score_name: column containing score for each patient
        display_name: label for X axis, default=score_name
        panel: name for panel in a grid plot e.g. "A", "Prescribing vs score"
        """

        if display_name == "":
            display_name = score_name

        sum_cols = [score_name] + drugs

        per_point = cohort[sum_cols].groupby(score_name).sum()
        per_point['date'] = per_point.index
        per_point = per_point.set_index('date')
        per_point['total'] = per_point[drugs].sum(axis=1)

        #normalise for stacked plot
        per_point_norm = per_point[drugs]
        for dr in drugs:
            norm = per_point[dr]/per_point['total']
            per_point_norm.loc[:,dr] = norm 
        per_point_norm = per_point_norm * 100
        per_point_norm['total'] = per_point['total'] #for label over column


        #colours2 = ['#b9433e','#f4b688','#90a0c7','#A1D292','#ffd92f','#A1D292']
        per_point_norm[drugs].plot(kind='bar', legend=False, stacked=True, ax=ax)

        #add n per bar
        for i in range(per_point_norm.shape[0]):
            ax.text(i-0.2, 100.5, int(per_point_norm['total'].iloc[i]), fontsize=8)

        #formatting axes and legend
        ax.tick_params(axis='x', rotation=0)
        ax.set_title("%s" % (panel), loc='left')
        ax.set_xlabel(display_name)
        ax.set_ylabel('Percent of admissions')
        patches, labels = ax.get_legend_handles_labels()
        ax.legend(patches, labels, loc=2, bbox_to_anchor=(1.05, 1), frameon=True, facecolor='#FFFFFF')
    
    def normalize_factor(self, cohort, factor_name):
        """
        Convert factor into standardized units and return cohort with new converted factor
        cohort: pandas dataframe for target cohort
        factor_name: column name for target variable to convert
        """
        col_output_name = str(factor_name) + "_z"
        cohort[col_output_name] = (cohort[factor_name] - cohort[factor_name].mean()) / cohort[factor_name].std()
        
        return cohort
    
    def run_regression(self, cohort, y, factors, add_constant = True, significance_level = 0.05):   
        """
        Run logistic regression target cohort for a list of factors
        cohort: pandas dataframe for target cohort
        y: column name that contains target output variable e.g. "any_at"
        factors: list of columns names that contain input variables
        add_constant: boolean on whether to add constant to regression
        significance_level: set significance threshold as integer for summary table
        """
        X = cohort[factors]

        if add_constant:
            X = sm.add_constant(X)

        X = X.astype(float)

        model = sm.Logit(y, X).fit()

        #get odds ratios with 95%CI
        params = model.params
        conf = model.conf_int()
        conf['OR'] = params
        conf.columns = ['Lower 95%CI', 'Upper 95%CI', 'OR']
        or_ci = np.exp(conf)
        or_ci = or_ci.round(2)
        or_ci['raw P-value'] = model.pvalues
        or_ci['Significant'] = or_ci['raw P-value'] < significance_level
        or_out = []
        p_out = []
        for index, row in or_ci.iterrows():
            s = "%0.1f (%0.1f-%0.1f)" % (row['OR'], row['Lower 95%CI'], row['Upper 95%CI'])
            or_out.append(s)
            if row['raw P-value'] < 0.001:
                p_out.append('<0.001')
            else:
                p_out.append("%0.3f" % row['raw P-value'])
        or_ci['P-value'] = p_out
        or_ci['OR (95%CI)'] = or_out
        if add_constant:
            or_ci.drop(['const'], inplace=True) #don't show the constant even if used

        return or_ci
        
    