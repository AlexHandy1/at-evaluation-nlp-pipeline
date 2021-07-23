import pandas as pd
import time
from datetime import datetime
import random
import json
from functools import reduce

class RiskScorer:
    def __init__(self):
        print("Initializing RiskScorer")
        
        self.cohort_individual_scores = []
        self.cohort_all_scores = {}
        
    
    def generate_score_definition_dict(self, score_codetable):
        '''
        Given a score_codetable as a pandas dataframe, return a structured score definition dict in format {"component": "points":X, "concepts":[]}
        score_codetable: pandas dataframe created from score definition csv file
        '''
        score_definition_dict = {key: {"points": 0, "concepts":[]} for key in list(set(score_codetable['component'].tolist()))}
        for idx, row in score_codetable.iterrows():
            score_definition_dict[row["component"]]["concepts"].append(row["cui"])
            score_definition_dict[row["component"]]["points"] = row["points"]
            
        return score_definition_dict
    
    def generate_component_maps(self, codetable):
        '''
        Create a set of unique components from a codelist table and a mapping of unique SNOMED-CT codes to target components
        codetable: pandas dataframe for codelist table
        '''
        components = set(codetable['component'].tolist())
        code_to_components = dict(zip(codetable["cui"], codetable["component"]))
     
        return components, code_to_components
    
    def generate_cohort_by_component_list(self, cohort, score_risk_components, score_unique_code_list, code_to_risk_component):
        '''
        Given a cohort, risk score components and the unique set of codes for the risk score components, create a dictionary of counts for annotations of each risk component for each patient and return a cohort_by_risk_component list.  
        cohort: pandas dataframe for target cohort
        score_risk_components: set of unique risk score components
        score_unique_code_list: set of unique codes for the risk score components
        code_to_risk_component: dictionary of codes to risk score components
        '''
        start = time.time()
        print("Start generating cohort by risk component table at: ", datetime.fromtimestamp(start))
        cohort_by_risk_component = []
        
        for doc in cohort:
            #setup components counter
            components_counter = { component:0 for component in score_risk_components }
            
            for ann in doc["annotations"]:
                if ann["cui"] in score_unique_code_list:
                    #check that the code is experienced by the patient (not "no stroke")
                    if ann["meta_anns"]["Negated"]["value"] == "No":
                        #map the code to the risk score component and add mention to count 
                        component = code_to_risk_component[ann["cui"]]
                        components_counter[component]+=1

            #setup document data entry
            doc_entry = {"doc_id":doc["doc_metadata"]["note_id"], "pat_id":doc["pat_metadata"]["pat_id"], "encounter_date":doc["doc_metadata"]["encounter_date"], "age":doc["pat_metadata"]["age"], "female": doc["pat_metadata"]["female"], "components": components_counter}

            #add to repackaged cohort for scoring
            cohort_by_risk_component.append(doc_entry)
        
        end = time.time()
        print("Finish generating cohort by risk component table at: ", datetime.fromtimestamp(end))
        print("Completed in %s minutes" % ( round(end - start,2) / 60 ) )
        
        return cohort_by_risk_component
    
 
    def add_non_coded_risk_score_components(self, cohort_by_risk_component, score_name):
        '''
        Given a list of dictionaries with annotations for target cohort, add columns for risk components without clinical codes (e.g. age, sex) and return a pandas dataframe ready for risk scoring. 
        cohort_by_risk_component: list of dictionaries with annotations for target cohort
        score_name: string for target risk score
        '''
        #Extract components and patient metadata
        cohort_comp = [pat["components"] for pat in cohort_by_risk_component]
        cohort_doc_ids = [pat["doc_id"] for pat in cohort_by_risk_component]
        cohort_pat_ids = [pat["pat_id"] for pat in cohort_by_risk_component]
        cohort_encounter_dates = [pat["encounter_date"] for pat in cohort_by_risk_component]
        cohort_ages = [pat["age"] for pat in cohort_by_risk_component]
        cohort_females = [pat["female"] for pat in cohort_by_risk_component]
        cohort_comp_df = pd.DataFrame(cohort_comp)
        
        cohort_comp_df["doc_id"] = cohort_doc_ids
        cohort_comp_df["pat_id"] = cohort_pat_ids
        cohort_comp_df["encounter_date"] = cohort_encounter_dates
        cohort_comp_df["age"] = cohort_ages
        cohort_comp_df["female"] = cohort_females
        
        if score_name == "chadsvasc":
            cohort_comp_df["age_65_74"] = cohort_comp_df["age"].apply(lambda x: (x >= 65) & (x < 75))
            cohort_comp_df["age_gte75"] = cohort_comp_df["age"].apply(lambda x: (x >= 75))
        else:
            cohort_comp_df["age_gt65"] = cohort_comp_df["age"].apply(lambda x: (x > 65))
        
        return cohort_comp_df
    
    def calculate_cohort_scores(self, df, score_definition_dict, score_name, identifiers=["doc_id"]):
        '''
        Given a cohort and a score definition return a dictionary of risk scores and errors
        
        df: pandas dataframe with patients as rows and parent concepts as columns. 
            Values are processed as truthy
        score_definition_dict: dictionary with definition for risk score
        score_name: string for target risk score
        identifiers: list of columns to be copied across to output table so 
            results can be joined.
            
        returns: dict of scores (dataframe, component points and total) and 
            errors (dict, concepts relevant to the score that were not detected 
            for any patients).
        '''
        
        score_df = pd.DataFrame()
        points_df = pd.DataFrame()
        
        concept_not_found = {}
        for s in score_definition_dict:
            #extract relevant columns from df
            component = df[s]
            score_df[s] = component.astype(bool)
            points_df[(s + "_" + score_name)] = score_df[s] * score_definition_dict[s]['points']
            
            #track missing concepts
            concept_not_found[s] = df[s].sum(axis=0) == 0

        total_score = points_df.sum(axis=1)
        points_df[('total' + "_" + score_name)] = total_score
        #copy across specified identifiers
        for idf in identifiers:
            points_df[idf] = df[idf]
        
        print("Not found", concept_not_found)
        
        return points_df
        
    
    def generate_individual_cohort_risk_score(self, cohort, score_definition, metadata_cols):
        '''
        When given a cohort and an array of score definition files calls other functions in pipeline on default settings to generate and return risk scores for cohort
        cohort: an annotated cohort in dictionary structure -> [{"pat_metadata":[], "doc_metadata":[], "annotations":[]}]
        score_definition: file path to csv file with the fields :component (phenotype category in risk score e.g. vascular disease), :cui (snomed-ct code for specific phenotype name within component e.g. S-95578000) and :name (specific name of phenotype e.g. renal vasculitis) :points (number of points for component in risk score)
        metadata_cols: list of column headers for metadata to be used as identifiers (e.g. ["doc_id", "pat_id"])
        '''
        
        score_codetable = pd.read_csv(score_definition)
        
        score_name = score_codetable["score"].unique()[0]

        print("Started risk scoring for ", score_name)
        
        score_definition_dict = self.generate_score_definition_dict(score_codetable)
        
        score_unique_code_list = set(score_codetable['cui'].tolist())
        
        score_risk_components, code_to_risk_component = self.generate_component_maps(score_codetable)
        
        cohort_by_risk_component = self.generate_cohort_by_component_list(cohort, score_risk_components, score_unique_code_list, code_to_risk_component)
        
        cohort_by_risk_component_table_with_metadata = self.add_non_coded_risk_score_components(cohort_by_risk_component, score_name)        

        cohort_scores = self.calculate_cohort_scores(cohort_by_risk_component_table_with_metadata, score_definition_dict, score_name, identifiers=metadata_cols)
        
        return cohort_scores
    
    def generate_cohort_scores(self, cohort, definitions):
        '''
        Top level convenience function to run generate_individual_cohort_risk_score function across a list of definitions for target cohort and return cohort as pandas dataframe
        cohort: an annotated cohort in dictionary structure -> [{"pat_metadata":[], "doc_metadata":[], "annotations":[]}]
        definitions: list of file paths to score_definition files
        '''
        start = time.time()
        print("Starting cohort risk scoring at: ", datetime.fromtimestamp(start))

        self.cohort_individual_scores = []
        
        metadata_cols = ["doc_id", "pat_id", "encounter_date", "age", "female"]
        
        for definition in definitions:
            scored_cohort = self.generate_individual_cohort_risk_score(cohort, definition, metadata_cols)
            self.cohort_individual_scores.append(scored_cohort)
                    
        self.cohort_all_scores = self.cohort_individual_scores[0].merge(self.cohort_individual_scores[1], how="left", on=metadata_cols)
        
        end = time.time()
        print("Cohort risk scoring finished at: ", datetime.fromtimestamp(end))
        print("Completed in %s minutes" % ( round(end - start,2) / 60 ) )
        
        return self.cohort_all_scores
    
    
    def generate_medication_flags(self, annotated_cohort, cohort_scores, medication_definition):
        '''
        Top level convenience function, when given a cohort and a medication definition file calls other functions in pipeline on default settings to generate and return medication counts for cohort added to risk scores
        annotated_cohort: an annotated cohort in dictionary structure -> [{"pat_metadata":[], "doc_metadata":[], "annotations":[]}]
        cohort_scores: dataframe of risk scores for a cohort
        medication_definition: a filepath to a codelist table in csv format with the fields :component (consistent, agreed name for medication e.g. Warfarin), :cui (snomed-ct code for specific medication name within component e.g. S-95578000) and :term (specific name of medication e.g. warfarin) 
        '''
        start = time.time()
        print("Starting cohort medication flag generation at: ", datetime.fromtimestamp(start))
        
        med_codetable = pd.read_csv(medication_definition)
        metadata_cols = ["doc_id", "pat_id", "encounter_date", "age", "female"]
        
        med_unique_code_list = set(med_codetable['cui'].tolist())
        
        med_components, code_to_med_component = self.generate_component_maps(med_codetable)
        
        cohort_by_med_component = self.generate_cohort_by_component_list(annotated_cohort, med_components, med_unique_code_list, code_to_med_component)
        
        cohort_scores_and_medication = self.add_medication_flags(cohort_scores,cohort_by_med_component, metadata_cols = metadata_cols)
        
        end = time.time()
        print("Cohort medication flags finished at: ", datetime.fromtimestamp(end))
        print("Completed in %s minutes" % ( round(end - start,2) / 60 ) )
        
        return cohort_scores_and_medication
    
    
    def add_medication_flags(self, cohort_scores, cohort_by_med_component, metadata_cols):
        '''
         Given a dataframe of cohort scores and a dataframe of medication counts, return a joined dataframe on patient ID
         cohort_scores: dataframe of risk scores for a cohort
         cohort_by_med_component: dataframe of medication counts for a cohort
         metadata_cols: list of column headers for metadata to be used as identifiers (e.g. ["doc_id", "pat_id"])
        '''
        #Extract components and patient ids
        cohort_comp = [pat["components"] for pat in cohort_by_med_component]
        cohort_doc_ids = [pat["doc_id"] for pat in cohort_by_med_component]
        cohort_pat_ids = [pat["pat_id"] for pat in cohort_by_med_component]
        cohort_encounter_dates = [pat["encounter_date"] for pat in cohort_by_med_component]
        cohort_ages = [pat["age"] for pat in cohort_by_med_component]
        cohort_females = [pat["female"] for pat in cohort_by_med_component]
        cohort_comp_df = pd.DataFrame(cohort_comp)
        
        cohort_comp_df["doc_id"] = cohort_doc_ids
        cohort_comp_df["pat_id"] = cohort_pat_ids
        cohort_comp_df["encounter_date"] = cohort_encounter_dates
        cohort_comp_df["age"] = cohort_ages
        cohort_comp_df["female"] = cohort_females
        
        cohort_scores_and_medication = cohort_scores.merge(cohort_comp_df, how="left", on=metadata_cols)
        return cohort_scores_and_medication