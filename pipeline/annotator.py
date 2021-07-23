import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
import logging
import psutil
import pipeline.config as config

log = logging.getLogger(__name__)

from medcat.cat import CAT
from medcat.utils.vocab import Vocab
from medcat.cdb import CDB
from tokenizers import ByteLevelBPETokenizer
from medcat.meta_cat import MetaCAT

class Annotator:
    def __init__(self, annotation_mode="MedCAT"):
        self.annotation_mode = annotation_mode
        print("Initializing " + self.annotation_mode + " as the annotator...")
        
        self.annotation_model = ""
        
        if self.annotation_mode == "MedCAT":
            start = time.time()
            print("Start loading MedCAT at: ", datetime.fromtimestamp(start))
            
            cdb_path = config.Config().medcat_config["cdb_path"]
            vocab_path = config.Config().medcat_config["vocab_path"]
            meta_path = config.Config().medcat_config["meta_path"]
            
            #NOTE - this api does not work with MedCAT v1
            cdb = CDB()
            cdb.load_dict(cdb_path)
            vocab = Vocab()
            vocab.load_dict(path=vocab_path)
            meta_neg = MetaCAT(save_dir=meta_path)
            meta_neg.load()
            
            self.annotation_model = CAT(cdb=cdb, vocab=vocab, meta_cats=[meta_neg])
            self.annotation_model.train = False 
            
            #NOTE - consider moving accuracy settings to config
            self.annotation_model.spacy_cat.MIN_ACC = 0.3
            self.annotation_model.spacy_cat.MIN_ACC_TH = 0.3
            self.annotation_model.spacy_cat.MIN_CONCEPT_LENGTH = 2
            
            end = time.time()
            print("Finish loading MedCAT at: ", datetime.fromtimestamp(end))
            print("Completed in %s minutes" % ( round(end - start,2) / 60 ) )
    
    def add_female_flag(self, x):
        '''
        Row wise operation to convert "Female" string entry to binary flag
        x: row entry for gender assumes "Female" or "Male" string
        '''
        if x == "Female":
            return 1
        else:
            return 0
        
    #NOTE - This function can be refactored if have ethics to include patient metadata in CogStack. At Trust where pipeline was developed this data had to be loaded in separately.
    def add_demographic_data(self, cohort, metadata_csv_file):
        '''
        Given a cohort (pre-annotation) and a csv with date of birth and gender for a list of patient mrns, map date of birth and gender data to patient mrns in cohort and return updated cohort. 
        Function also applies a series of demographic filters (removes Na's from date of birth and gender, removes legacy dates out of time range, removes individuals with age >= 18 years old)
        cohort: pandas dataframe of cohort extracted from CogStack
        metadata_csv_file: filepath with fields primary_mrn, date_of_birth and gender
        '''
        metadata = pd.read_csv(metadata_csv_file)
        print("Cohort size prior to joining age and gender metadata", len(cohort))
        
        metadata["patientprimarymrn"] = metadata["primary_mrn"]
        
        cohort = pd.merge(cohort, metadata, on="patientprimarymrn", how="left")
        print("Cohort size post joining age and gender metadata", len(cohort))
        
        #filter out Na's from dob and gender
        print("Cohort size prior to removing dob and gender na's", len(cohort))
        cohort = cohort[cohort['gender'].notna()]
        cohort = cohort[cohort['date_of_birth'].notna()]
        print("Cohort size post removing dob and gender na's", len(cohort))
        
        #add binary flag for female
        cohort["female"] = cohort["gender"].apply(self.add_female_flag)
        
        #add age from date of birth
        today = pd.to_datetime("today")
        cohort['date_of_birth_dt'] = pd.to_datetime(cohort['date_of_birth'])
        cohort["age"] = (today - cohort['date_of_birth_dt']) / np.timedelta64(1, 'Y')
        
        #check and remove any encounter dates outside of time range (01/01/2011 - 01/01/2019)
        cohort["encounterdate_dt"] = pd.to_datetime(cohort["encounterdate"])
        print("# dates < 01/01/2011", len(cohort[cohort["encounterdate_dt"] < pd.to_datetime('01/01/2011')]))
        print("# dates > 01/01/2019", len(cohort[cohort["encounterdate_dt"] > pd.to_datetime('01/01/2019')]))
        print("Cohort size prior to removing dates outside of time range", len(cohort))
        cohort = cohort[cohort["encounterdate_dt"] < pd.to_datetime('01/01/2019')]
        cohort = cohort[cohort["encounterdate_dt"] >= pd.to_datetime('01/01/2011')]
        print("Cohort size post removing dates outside of time range", len(cohort))
        
        
        #remove individuals with age >= 18 years old
        print("Cohort size prior to removing age >=18 years old", len(cohort))
        cohort = cohort[cohort["age"] >= 18]
        print("Cohort size post removing age >=18 years old", len(cohort))
        
        #reset index
        cohort = cohort.reset_index().iloc[:, 1:]
        
        print("Cohort size for annotation", len(cohort))
        
        return cohort
        
    def add_pat_metadata(self, doc):
        '''
        Extract patient metadata from input cohort
        doc: document in string format from target cohort, assumes known column labels for input cohort dataframe
        '''
        pat_metadata = {}
        pat_metadata["pat_id"] = doc["patientprimarymrn"]
        pat_metadata["age"] = doc["age"]
        pat_metadata["female"] = doc["female"]
        
        return pat_metadata
    
    def add_doc_metadata(self, doc):
        '''
        Extract document metadata from input cohort
        doc: document in string format from target cohort, assumes known column labels for input cohort dataframe
        '''
        doc_metadata = {}     
        doc_metadata["note_id"] = doc["clinicalnotekey"]
        doc_metadata["encounter_date"] = doc["encounterdate"]

        return doc_metadata
    
    def add_annotations(self, doc):
        '''
        Apply annotations to document using loaded annotation model and return an array of annotations
        doc: document in string format from target cohort, assumes column label "notetext" for input cohort dataframe
        '''
        try: 
            annotations = self.annotation_model.get_entities(doc["notetext"])
        except Exception as e: 
            print(e)
            print('failed on ', doc["clinicalnotekey"])
            log.error('failed on ' + str(doc["clinicalnotekey"]))
        
        return annotations
    
    #NOTE - This function (specifically the metadata_csv_file parameter) can be refactored if have ethics to include patient metadata in CogStack. At Trust where pipeline was developed this data had to be loaded in separately.
    #In next version will aim to build in flexibility around this metadata parameter
    def annotate_cohort(self, cohort, metadata_csv_file):
        '''
        Top level convenience function, when given a cohort calls other functions in pipeline on default settings to annotate cohort
        cohort: a pandas dataframe with target cohort information (patient metadata [where available], document metadata and note text)
        metadata_csv_file: filepath with fields primary_mrn, date_of_birth and gender
        '''
        start = time.time()
        print("Starting cohort annotation at: ", datetime.fromtimestamp(start))
        
        cohort = self.add_demographic_data(cohort, metadata_csv_file)
        
        annotated_cohort = []
        
        for idx, doc in cohort.iterrows():
            memory_available = psutil.virtual_memory().available / (1024.0 ** 3)
            if idx % 100 == 0:
                print("Completed up to index:", idx, " ", (len(cohort) - idx), "left to process")
                print("GB memory available: ", str(memory_available))
            
            log.debug("Index:" + str(idx))
            log.debug("Doc length:" + str(len(doc["notetext"])))
            log.debug("GB memory available:" + str(memory_available))
            
            doc_entry = {}
            doc_entry["pat_metadata"] = self.add_pat_metadata(doc)
            doc_entry["doc_metadata"] = self.add_doc_metadata(doc)
            doc_entry["annotations"] = self.add_annotations(doc)
            
            annotated_cohort.append(doc_entry)
            
            log.debug("Total document list length:" + str(len(annotated_cohort)))
            
        end = time.time()
        print("Cohort annotation finished at: ", datetime.fromtimestamp(end))
        print("Completed in %s minutes" % ( round(end - start,2) / 60 ) )
        
        return annotated_cohort
        