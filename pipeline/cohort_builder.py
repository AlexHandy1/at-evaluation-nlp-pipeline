import pipeline.esconn as esconn
import pipeline.config as config
import pandas as pd
import time
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import os
import ssl

class CohortBuilder:
    def __init__(self, server="gae03"):
        print("Initializing Cohort Builder")
        
        print("Connect to ES...")
        print("Connecting to ES on server: " + server)
        
        #NOTE - this reflects the bespoke server setup where pipeline was developed and will require refactoring for clearer interpretation and implementation at other NHS Trusts
        if server == "gae03":
            es_ssl_config = esconn.SslConnectionConfig(ca_certs_path=config.Config().es_config["gae03_ca_certs_path"])
            es_conn_config = esconn.ElasticConnectorConfig(hosts=config.Config().es_config["gae03_hosts"], 
                                                           port=config.Config().es_config["gae03_port"], 
                                                           user_name=config.Config().es_config["gae03_user"], 
                                                           user_pass=config.Config().es_config["gae03_password"], 
                                                           ssl_config=es_ssl_config)

            es_conn = esconn.ElasticConnector(es_conn_config)
            es_conn = es_conn.es
        #target index
        elif server == "gae02":
            es_conn = Elasticsearch(['http://uclvlddpragae02:9200'], http_auth=(config.Config().es_config["gae02_user"],config.Config().es_config["gae02_password"]))
        else:
            print("Please try another server which has CogStack data")
            es_conn = None
        
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
        
        if "note_type" in kwargs:
            try: 
                cohort = cohort[cohort["notetype"] == kwargs["note_type"]]
            except:
                print("No note type field available")
        
        print("Number of rows pre individual selection:", len(cohort))
        cohort = self.select_most_recent_patient_doc(cohort)
        print("Number of rows post individual selection:", len(cohort))
       
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
        
        if "note_type" in kwargs:
            cohort = self.package_cohort(es_response, note_type = kwargs["note_type"])
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
            