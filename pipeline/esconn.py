#!/usr/bin/python

import elasticsearch
import elasticsearch.helpers
import ssl
import requests
import logging


################################
#
# connector config
#
class SslConnectionConfig:
    """
    SSL connection configuration for ElasticSearch
    """
    def __init__(self, ca_certs_path, client_cert_path = None, client_key_path = None):
        """
        :param ca_certs_path: path for CA cert file (PEM)
        :param ca_certs_path: path for client cert file (PEM)
        :param ca_certs_path: path for client key file (PEM)
        """
        self.ca_certs_path = ca_certs_path
        self.client_cert_path = client_cert_path
        self.client_key_path = client_key_path


class ElasticConnectorConfig:
    """
    ElasticSearch connector configuration
    All the hosts details are specified using RFC-1738
    """
    def __init__(self, hosts, port = '9200', user_name = None, user_pass = None, ssl_config=None):
        """
        :param hosts: the ElasticSearch host names
        """
        self.hosts = hosts
        self.port = port
        self.user_name = user_name
        self.user_pass = user_pass
        self.ssl_config = ssl_config


################################
#
# connector
#
class ElasticConnector:
    """
    ElasticSearch connector
    At the moment supports only single-node clusters
    """
    def __init__(self, elastic_conf):
        """
        :param elastic_conf: ElasticSearch configuration :class:`~ElasticConnectorConfig`
        """
        # check whether we can actually connect to ElasticSearch
        try:
            if elastic_conf.ssl_config is not None:
                # this has been deprecated:
                """"
                self.es = elasticsearch.Elasticsearch(hosts=elastic_conf.hosts,
                                                      use_ssl=True,
                                                      verify_certs=True,
                                                      ca_certs=elastic_conf.ssl_config.ca_certs_path,
                                                      client_cert=elastic_conf.ssl_config.client_cert_path,
                                                      client_key=elastic_conf.ssl_config.client_key_path,
                                                      retry_on_timeout=True)
                """
                # need to apply http proxy bypass on GAEs due to provided in system external ones
                proxy_bypass = {'https': '', 'http' : ''}
                
                context = ssl.create_default_context(cafile=elastic_conf.ssl_config.ca_certs_path)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE

                if elastic_conf.user_name is not None and elastic_conf.user_pass is not None:
                    self.es = elasticsearch.Elasticsearch(
                        hosts=elastic_conf.hosts,
                        http_auth=(elastic_conf.user_name, elastic_conf.user_pass),
                        scheme="https",
                        port=elastic_conf.port,
                        ssl_context=context,
                        retry_on_timeout=True)
                else:
                    # usually not the case
                    self.es = elasticsearch.Elasticsearch(
                        hosts=elastic_conf.es_hosts,
                        scheme="https",
                        port=elastic_conf.port,
                        ssl_context=context,
                        retry_on_timeout=True)
            else:
                self.es = elasticsearch.Elasticsearch(hosts=elastic_conf.hosts, retry_on_timeout=True)
        except Exception as e:
            raise Exception("Cannot connect to ElasticSearch: %s" % str(elastic_conf.hosts), e)
            
        if self.es.ping() is not True:
            raise Exception("Cannot connect to ElasticSearch: %s" % str(elastic_conf.hosts))
            
            
    def __del__(self):
        """Just a placeholder. No need to close the connector"""
        pass
        
        
    def check_connection(self):
        return self.es.ping()


################################
#
# indexer
#
class ElasticIndexer:
    """
    ElasticSearch indexer
    """

    # the size of chunk when indexing documents in bulk
    BULK_CHUNK_SIZE = 5000
    BULK_REQUEST_TIMEOUT_S = 30

    def __init__(self, es_connector, index_name):
        """
        :param es_connector: ElasticSearch connector :class:`~ElasticConnector`
        :param index_name: the name of the index
        """
        self.conn = es_connector
        self.index_name = index_name

        # deprecated in 6.x so use it as a build-in param here
        self.doc_type = 'doc'

        self.index_name_cache = {}

        self.log = logging.getLogger('ElasticIndexer')

    def _format_index_name(self, index_name):
        """
        ElasticSearch index name must follow certain rules:
        - must not contain the characters #, \, /, *, ?, ", <, >, |, ,
        - must not start with _, - or +
        - must not be . or ..
        - must be lowercase
        :param index_name: a possible index name
        :return: correct index name
        """
        return index_name.lower()\
            .strip('.').strip('_').strip('-').strip('+')\
            .replace('#', '_').replace('\\', '_').replace('\/', '_')\
            .replace('*', '_').replace('?', '_').replace('"', '_')\
            .replace('<', '_').replace('>', '_').replace('|', '_')\
            .replace(' ', '_')

    def get_index_name(self, suffix="", search_only=False):
        """
        Returns the valid index name taking into account suffix and naming restrictions
        :param suffix: an optional index suffix name
        :param search_only: True if only using search
        :return: valid index name
        """
        if len(suffix) > 0:
            if search_only and suffix is "*":
                return "%s-*" % self.index_name

            index_name = "%s-%s" % (self.index_name, suffix)
        else:
            index_name = self.index_name

        if index_name not in self.index_name_cache:
            self.index_name_cache[index_name] = self._format_index_name(index_name)

        return self.index_name_cache[index_name]

    def get_doc_count(self, index_suffix=""):
        """
        Queries the index for the number of documents (_count)
        :param index_suffix: an optional suffix of the index to query
        :return: the number of records
        """
        res = self.conn.es.count(index=self.get_index_name(suffix=index_suffix, search_only=True))
        return int(res['count'])

    def drop_index(self, index_suffix=""):
        """
        Drops the specified index
        :param index_suffix: optional suffix of the index to drop
        """
        self.conn.es.indices.delete(index=self.get_index_name(index_suffix))

    def index_doc(self, doc, doc_id=None, index_suffix=""):
        """
        Indexes the given document under under specified id
        :param doc: the body of the document to be stored (must be represented as KVPs dictionary)
        :param doc_id: the id of the document
        :param index_suffix: optional suffix of the index to store the document
        """
        try:
            self.conn.es.index(index=self.get_index_name(index_suffix), doc_type=self.doc_type, id=doc_id, body=doc)
        except Exception as e:
            self.log.error("Exception caught while indexing document: " + str(e))

    def index_docs_bulk(self, docs, index_suffix=""):
        """
        Indexes the documents using ElasticSearch bulk API
        :param docs: the array of documents, each represented as KVPs
        :param index_suffix: an optional index suffix name
        """
        index_name = self.get_index_name(index_suffix)
        try:
            elasticsearch.helpers.bulk(self.conn.es, docs, index=index_name, doc_type=self.doc_type)
        except Exception as e:
            self.log.error("Exception caught while indexing documents in bulk: " + str(e))

    def index_docs_bulk_gen(self, actions_generator):
        """
        Indexes the documents using ElasticSearch bulk API
        :param actions_generator: the generator of documents, must include the index name
        """
        failed_docs = 0
        try:
            for status, result in elasticsearch.helpers.streaming_bulk(self.conn.es,
                                                                       actions=actions_generator,
                                                                       chunk_size=self.BULK_CHUNK_SIZE,
                                                                       request_timeout=self.BULK_REQUEST_TIMEOUT_S):
                if status is False:
                    failed_docs += 1

            if failed_docs:
                self.log.warn("Failed indexing documents in bulk: %d " % failed_docs)

        except Exception as e:
            self.log.error("Exception caught while indexing documents in bulk: " + str(e))

    def get_doc(self, doc_id, index_suffix=""):
        """
        Retrieves the given document from a given index with a specified id
        :param doc_id: the id of the document
        :param index_suffix: optional suffix of the index to store the document
        :return: the document represented as KVPs dictionary
        """
        res = self.conn.es.get(index=self.get_index_name(index_suffix), doc_type=self.doc_type, id=doc_id)
        assert '_source' in res
        return res['_source']

    def get_doc_ids(self, index_suffix=""):
        """
        Retrieves the ids of all the documents
        :param index_suffix: optional suffix of the index to store the document
        :return: the document ids in array
        """
        query_body = {
            "query": {
                "match_all": {}
            },
            "stored_fields": []
        }

        meta = self.conn.es.search(index=self.get_index_name(suffix=index_suffix, search_only=True),
                                   body=query_body)
        if 'hits' not in meta:
            return []

        ids = [hit['_id'] for hit in meta['hits']['hits']]
        return ids

    def doc_exists(self, match_criteria, index_suffix=""):
        """
        Checks whether the document exists according to match criterion
        :param match_criteria: the match criteria specified as field-value KVPs
        :param index_suffix: optional suffix of the index to store the document
        :return: True if document exists, False otherwise
        """
        query_body = {
            "query": {
                "match": match_criteria
            }
        }
        index_name = self.get_index_name(suffix=index_suffix, search_only=True)
        if not self.conn.es.indices.exists(index=index_name):
            return False

        res = self.conn.es.count(index=index_name, body=query_body)
        return int(res['count']) > 0

    def get_doc_ids_scan(self, index_suffix=""):
        """
        Retrieves the ids of all the documents using ElasticSearch scan API
        :param index_suffix: optional suffix of the index to store the document
        :return: the document ids in array
        """
        query_body = {
            "query": {
                "match_all": {}
            },
            "stored_fields": []
        }

        ids_generator = elasticsearch.helpers.scan(self.conn.es,
                                                   query=query_body,
                                                   index=self.get_index_name(suffix=index_suffix,
                                                                             search_only=True),
                                                   doc_type=self.doc_type)
        ids = [hit['_id'] for hit in ids_generator]
        return ids


################################
#
# ranged indexer
#
class ElasticRangedIndexer(ElasticIndexer):
    def __init__(self, es_connector, index_name):
        super().__init__(es_connector, index_name)

    def get_doc_ids_by_range_scan(self, date_field, date_begin, date_end, date_format="yyyy-MM-dd", index_suffix=""):
        """
        Retrieves the ids of all the documents using ElasticSearch scan API
        :param date_field: the name of the field containing the date
        :param date_begin: begin of the range, inclusive
        :param date_end: optional suffix of the index to store the document
        :param date_format: the format of the date field
        :param index_suffix: end of the range, inclusive
        :return: the document ids in array
        """
        query_body = {
            "query": {
                "range": {
                    date_field: {
                        "gte": date_begin,
                        "lte": date_end,
                        "format": date_format
                    }
                }
            },
            "stored_fields": []
        }

        ids_generator = elasticsearch.helpers.scan(self.conn.es,
                                                   query=query_body,
                                                   index=self.get_index_name(index_suffix),
                                                   doc_type=self.doc_type)
        ids = [hit['_id'] for hit in ids_generator]
        return ids