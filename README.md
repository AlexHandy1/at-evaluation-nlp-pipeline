# A natural language processing pipeline to evaluate antithrombotic use in multi-hospital atrial fibrillation cohort 

This repository provides a natural language processing (NLP) based analysis pipeline to evaluate antithrombotic use in individuals with atrial fibrillation using discharge summaries from hospital electronic health records.    

The pipeline builds on existing opensource NLP software, specifically [CogStack](https://github.com/CogStack/CogStack-Pipeline) for document storage and [MedCAT](https://github.com/CogStack/MedCAT) for document annotation.   


The code is designed to be run within a Docker container than can be built from the Dockerfile provided. The infrastructure pre-requisites to run this code are a server with Docker and CogStack installed. Target discharge summaries must then be ingested into the CogStack instance and the `config.py` file updated with the relevant Elasticsearch index.   

Metadata for the discharge summaries should also be available and mapped to the following fields in CogStack:  
- "clinicalnotekey": unique document ID  
- "patientprimarymrn": unique patient ID for document  
- "encounterdate": date document was created  
- "gender": recorded gender of the patient  
- "date_of_birth": recorded date of birth of the patient  
- "notetext": free text from the document. This may need to be pre-processed using a service such as [Apache Tika](https://tika.apache.org/) if documents are stored as PDFs.  

All other data (e.g. risk scores, medication summaries) will be created and summarised automatically from document annotations.   

Sensitive, NHS Trust specific implementation details (e.g. passwords, server configurations, pre-trained annotation models) are not included in this repository so the code will not work out of the box.
Please get in touch with a.handy@ucl.ac.uk if you would like to install this pipeline at your hospital.  

