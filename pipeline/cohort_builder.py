import pipeline.esconn as esconn
import pipeline.config as config
import pandas as pd
import time
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import os
import ssl

class CohortBuilder:
    def __init__(self):
        print("Initializing Cohort Builder")
        
        print("Connect to ES...")

        es_conn = Elasticsearch([config.Config().es_config["es_url"]], http_auth=(config.Config().es_config["es_user"],config.Config().es_config["es_password"]))
        
        if es_conn.ping() is True:
            print("Connected to ES")
    
        self.es = es_conn
        
    def construct_query(self, search_term, batch_size=10000):
        '''
        Given a search term and batch size return a structured ES search query for use in search api
        search_term: a string term to conduct text search with
        batch_size: an integer for the number of documents to be processed in each batch of scroll API (should not require altering)
        '''
        query = {"size":batch_size,
         "query":{"bool":
                    {"must":[{"query_string":
                                {"query": search_term + '*',
                                "analyze_wildcard":True,
                                "default_field":"*"}}],
                    "filter":[],
                    "should":[],
                    "must_not":[]}}}
        
        return query
    
    def query_es(self, query, index):
        '''
        Given a structured ES search query return an array of results using the scroll API
        query: an array containing a structured ES search query
        index: an ES index that hosts target documents
        
        '''
        es_response = []
        res = helpers.scan(
                client = self.es,
                scroll = '2m',
                query = query, 
                index = index)
        
        for doc in res:
            es_response.append(doc)
        
        return es_response
    
    #NOTE: these are a set of optional functions for removing invalid discharge summaries at UCLH which could only be filtered with access to the free text note. They will not be relevant at other Trusts.
    def remove_stroke_ds(self, x):
        '''
        Given a cohort note text, add flag for stroke pad discharge summaries at UCLH
        x: row with notetext data
        '''
        target = x[0:7]
        target_cl = target.strip()

        if target_cl == "Please":
            return True
        else:
            return False
        
    def remove_emergency_ds(self, x):
        '''
        Given a cohort note text, add flag for emergency discharge summaries at UCLH
        x: row with notetext data
        '''
        target = x[0:41]
        target_cl = target.strip()

        if target_cl == "Discharge Summary (Emergency Department)":
            return True
        else:
            return False
        
    def remove_cc_ds(self, x):
        '''
        Given a cohort note text, add flag for critical care discharge summaries at UCLH
        x: row with notetext data
        '''
        if "UCH Critical Care Discharge Summary" in x:
            return True
        else:
            return False
        
    def add_keep_doc_flag(self, x):
        '''
        Given a cohort as a pandas dataframe, add keep doc flag based on previously assigned invalid discharge summary columns
        x: row with all columns for cohort
        '''
        if x["invalid_stroke_ds"] or x["invalid_emergency_ds"] or x["invalid_cc_ds"]:
            return False
        else:
            return True
    
    #NOTE: functions relevant across Trusts from here again
    def select_most_recent_patient_doc(self, cohort):
        '''
        Given a cohort as a pandas dataframe, return an individual entry for each patient id based on most recent encounter date
        cohort: pandas dataframe containining target cohort
        '''
        #convert to datetime
        cohort["encounterdate_dt"] = pd.to_datetime(cohort["encounterdate"]) 
        
        #remove na's for dates
        cohort = cohort[cohort['encounterdate_dt'].notna()]
        
        #select individual patients
        cohort = cohort.sort_values('encounterdate_dt').groupby('patientprimarymrn').tail(1)
        
        #print distribution of dates as a check
        print("Distribution of cohort dates: ", cohort["encounterdate_dt"].describe())
        
        return cohort
    
    def package_cohort(self, es_response, **kwargs):
        '''
        Given an array of ES results and an optional kwargs flag for note_type, return a pandas dataframe filtered by target note_type and with an individual entry for each patient id based on most recent encounter date
        es_response: an array containing a set of results from ES
        '''
        es_response_source_docs = []
        es_response_source_docs.extend([result['_source'] for result in es_response])
        cohort = pd.DataFrame(es_response_source_docs)
        
        if "trust_site" in kwargs:
            if kwargs["trust_site"] == "UCLH":
                print("Remove strokepad, emergency department and critical care discharge summaries")
                print("Number of rows pre invalid discharge summary removal:", len(cohort))
                print("Number of individuals pre invalid discharge summary removal:",len(cohort.groupby("patientprimarymrn").count()))
                
                cohort["invalid_stroke_ds"] = cohort["notetext"].apply(self.remove_stroke_ds)
                cohort["invalid_emergency_ds"] = cohort["notetext"].apply(self.remove_emergency_ds)
                cohort["invalid_cc_ds"] = cohort["notetext"].apply(self.remove_cc_ds)
                
                pct_stroke_ds = len(cohort[cohort["invalid_stroke_ds"]]) / len(cohort)
                pct_emergency_ds = len(cohort[cohort["invalid_emergency_ds"]]) / len(cohort)
                pct_cc_ds = len(cohort[cohort["invalid_cc_ds"]]) / len(cohort)
                print("Invalid stroke ds %: ", pct_stroke_ds)
                print("Invalid emergency ds %: ", pct_emergency_ds)
                print("Invalid critical care ds %: ", pct_cc_ds)
                
                cohort["keep_doc"] = cohort.apply(self.add_keep_doc_flag, axis=1)
                
                cohort = cohort[cohort["keep_doc"]]
                cohort = cohort.reset_index()

                print("Number of rows post invalid discharge summary removal:", len(cohort))
                print("Number of individuals post invalid discharge summary removal:",len(cohort.groupby("patientprimarymrn").count()))
                
        print("Number of rows pre most recent document selection:", len(cohort))
        print("Number of individuals pre most recent document selection:",len(cohort.groupby("patientprimarymrn").count()))
        cohort = self.select_most_recent_patient_doc(cohort)
        print("Number of rows post most recent document selection:", len(cohort))
        print("Number of individuals post most recent document selection:",len(cohort.groupby("patientprimarymrn").count()))
       
        return cohort
        
    def get_cohort_size(self, cohort):
        '''
        For a generated cohort, calculates number of documents and unique patients based on id and prints these numbers to the terminal
        cohort: pandas dataframe containining target cohort
        '''
        n_documents = len(cohort)
        n_patients = len(cohort.groupby("patientprimarymrn").count())
        
        print("Number of documents:", n_documents)
        print("Number of patients:", n_patients)
        

    def build_cohort(self, search_term, index="nifi_epic_raw_notes", batch_size=10000, **kwargs):
        '''
        Top level convenience function, when given search term, index, batch_size and optional kwargs (e.g. note_type) calls other functions in pipeline to build target cohort
        search_term: a string term to conduct text search with
        index: a string with the ES index hosting target documents
        batch_size: an integer for the number of documents to be processed in each batch of scroll API (should not require altering)
        optional kwargs flag for note_type
        '''
        start = time.time()
        print("Starting cohort build at: ", datetime.fromtimestamp(start))
        
        query = self.construct_query(search_term, batch_size)
        es_response = self.query_es(query, index)
            
        if "trust_site" in kwargs:
            cohort = self.package_cohort(es_response, trust_site = kwargs["trust_site"])
        else:
            cohort = self.package_cohort(es_response)
        
                
        self.get_cohort_size(cohort)
        end = time.time()
        print("Cohort build finished at: ", datetime.fromtimestamp(end))
        print("Completed in %s minutes" % ( round(end - start,2) / 60 ) )
        return cohort
    
    #NOTE - legacy function when was combining cohorts from different ES indices, should not be required
    def combine_cohorts(self, cohorts):
        '''
        Given an array of cohorts as pandas dataframe, return one combined cohort with each cohort appended and an individual entry for each patient id based on most recent encounter date
        cohorts: array of cohorts, each cohort assumed to be pandas dataframe 
        '''
        
        combined_input_len = sum([ len(cohort) for cohort in cohorts ])
        
        print("Number of rows pre join:", combined_input_len)
        
        joint_cohort = pd.concat(cohorts).reset_index().sort_values('encounterdate_dt').groupby('patientprimarymrn').tail(1)
        
        print("Number of rows post join:", len(joint_cohort))
        
        return joint_cohort
            