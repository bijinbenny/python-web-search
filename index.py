#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
API - a simple web search engine.
The goal is to index an infinite list of URLs (web pages), and then be able to quickly search relevant URLs against a query.

- Indexing :
The indexing operation of a new URL first crawls URL, then extracts the title and main text content from the page.
Then, a new document representing the URL's data is saved in ElasticSearch, and goes for indexing.

- Searching :
When searching for relevant URLs, the search engine will compare the query with the data in each document (web page),
and retrieve a list of URLs matching the query and sorted by relevance.

This API works for a finite list of languages, see here for the complete list : https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-lang-analyzer.html.
"""

__author__ = "Anthony Sigogne"
__copyright__ = "Copyright 2017, Byprog"
__email__ = "anthony@byprog.com"
__license__ = "MIT"
__version__ = "1.0"

import re
import os
import url
import crawler
import requests
import json
import query
from flask import Flask, request, jsonify
#from elasticsearch_dsl.connections import connections
#from elasticsearch_dsl import Index, Mapping
from language import languages
from redis import Redis
from rq import Queue
from multiprocessing import Process
from multiprocessing import Queue as Q
from twisted.internet import reactor
from rq.decorators import job
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
from urllib.parse import urlparse
from datetime import datetime
from elasticsearch import Elasticsearch
from flask_rq2 import RQ
import logging
from twisted.internet import reactor


# init flask app and import helper
app = Flask(__name__)
with app.app_context():
    from helper import *
app.config['RQ_REDIS_URL']='redis://localhost:6379/0'
# initiate the elasticsearch connection
hosts = [os.getenv("HOST")]
http_auth = (os.getenv("USERNAME"), os.getenv("PASSWORD"))
port = os.getenv("PORT")
#client = connections.create_connection(hosts=hosts, http_auth=http_auth, port=port)
logging.basicConfig(filename=datetime.now().strftime('server_%d_%m_%Y.log'),level=logging.DEBUG,format='%(asctime)s %(levelname)-8s %(message)s')

# initiate Redis connection
#redis_conn = RQ(os.getenv("REDIS_HOST", "redis"), os.getenv("REDIS_PORT", 6379))
redis_conn = RQ(app)

# create indices and mappings

es = Elasticsearch(hosts="http://bijin:Samsung1!@localhost:9200/")
logging.info(es.info())
#for lang in ["en"] : #languages :
    # index named "web-<language code>"
#    index = Index('web-%s'%lang)
 #   if not index.exists() :
  #      index.create()
settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
            "properties": {
                "url": {
                    "type": "keyword"
                },
                "domain":{
                    "type": "keyword"
                },
                "title":{
                    "type": "text",
                    "analyzer": "english"
                },
                "description":{
                    "type": "text",
                    "analyzer": "english"
                },
                "body":{
                    "type": "text",
                    "analyzer": "english"
                },
                "weight":{
                    "type": "long"
                },
                "text_vector":{
                    "type": "dense_vector",
                    "dims": 768
                }
            }
    }
    
}
#es.indices.create(index='web-en',ignore=400,body=settings)
    # mapping of page
   # m = Mapping('page')
   # m.field('url', 'keyword')
   # m.field('domain', 'keyword')
   # m.field('title', 'text', analyzer=languages[lang])
   # m.field('description', 'text', analyzer=languages[lang])
   # m.field('body', 'text', analyzer=languages[lang])
   # m.field('weight', 'long')
    #m.field('thumbnail', 'binary')
    #m.field('keywords', 'completion') # -- TEST -- #
    #m.save('web-%s'%lang)

# index for misc mappings
#index = Index('web')
#if not index.exists() :
#    index.create()
settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
          "properties": {
                "homepage": {
                    "type": "keyword"
                },
                "domain": {
                    "type": "keyword"
                },
                "email": {
                    "type": "keyword"
                },
                "last_crawl": {
                    "type": "date"
                }
          }      
    }      
    
}
es.indices.create(index='web',ignore=400,body=settings)
# mapping of domain
# m = Mapping('domain')
# m.field('homepage', 'keyword')
# m.field('domain', 'keyword')
# m.field('email', 'keyword')
# m.field('last_crawl', 'date')
# m.field('keywords', 'text', analyzer=languages[lang])
# m.save('web')

@app.route("/index", methods=['POST'])
def index():
    """
    URL : /index
    Index a new URL in search engine.
    Method : POST
    Form data :
        - url : the url to index [string, required]
    Return a success message.
    """
    # get POST data
    data = dict((key, request.form.get(key)) for key in request.form.keys())
    if "url" not in data :
        raise InvalidUsage('No url specified in POST data')

    # launch exploration job
    index_job.delay(data["url"])

    return "Indexing started"

@job('default', connection=redis_conn)
def index_job(link) :
    """
    Index a single page.
    """
    print("index page : %s"%link)

    # get final url after possible redictions
    try :
        link = url.crawl(link).url
    except :
        return 0

    process = CrawlerProcess({
        'USER_AGENT': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36",
        'DOWNLOAD_TIMEOUT':100,
        'REDIRECT_ENABLED':False,
        'SPIDER_MIDDLEWARES' : {
            'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware':True
        }
    })
    process.crawl(crawler.SingleSpider, start_urls=[link,], es_client=es, redis_conn=redis_conn)
    process.start() # block until finished

@app.route("/explore", methods=['POST'])
def explore():
    """
    URL : /explore
    Explore a website and index all urls
    Method : POST
    Form data :
        - url : the url to explore [string, required]
    Return a success message (means redis-rq process launched).
    """
    # get POST data
    data = dict((key, request.form.get(key)) for key in request.form.keys())
    if "url" not in data :
        raise InvalidUsage('No url specified in POST data')

    logging.info("launch exploration job")
    job = explore_job.queue(data["url"])
    job.perform()

    return "Exploration started"

@redis_conn.job('low')
def explore_job(link) :
    """
    Explore a website and index all urls (redis-rq process).
    """
    logging.info("explore website at : %s"%link)

    # get final url after possible redictions
    try :
        link = url.crawl(link).url
    except :
        return 0

    # create or update domain data
    domain = url.domain(link)
    res = es.index(index="web",id=domain, body={
        "homepage":link,
        "domain":domain,
        "last_crawl":datetime.now()
    })
    def f(q):
        try:
            # start crawler
            runner = CrawlerRunner({
                'USER_AGENT': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36",
                'DOWNLOAD_TIMEOUT':100,
                'DOWNLOAD_DELAY':0.25,
                'ROBOTSTXT_OBEY':True,
                'HTTPCACHE_ENABLED':False,
                'REDIRECT_ENABLED':False,
                'SPIDER_MIDDLEWARES' : {
                    'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware':True,
                    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware':True,
                    'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware':True,
                    'scrapy.extensions.closespider.CloseSpider':True
                },
                'CLOSESPIDER_PAGECOUNT':500 #only for debug
            })
            runner.crawl(crawler.Crawler, allowed_domains=[urlparse(link).netloc], start_urls = [link,], es_client=es, redis_conn=redis_conn)
            d = runner.join()
            d.addBoth(lambda _: reactor.stop())

            reactor.run()
            q.put(None)
        except Exception as e:
            q.put(e)
    q = Q()
    p = Process(target=f, args=(q,))
    p.start()
    result = q.get()
    p.join()

    if result is not None:
        raise result        
    return 1

@app.route("/reference", methods=['POST'])
def reference():
    """
    URL : /reference
    Request the referencing of a website.
    Method : POST
    Form data :
        - url : url to website
        - email : contact email
    Return a success message.
    """
    # get POST data
    data = dict((key, request.form.get(key)) for key in request.form.keys())
    if "url" not in data or "email" not in data :
        raise InvalidUsage('No url or email specified in POST data')

    # launch exploration job
    reference_job.delay(data["url"], data["email"])

    return "Referencing started"

@job('default', connection=redis_conn)
def reference_job(link, email) :
    """
    Request the referencing of a website.
    """
    print("referencing page %s with email %s"%(link,email))

    # get final url after possible redictions
    try :
        link = url.crawl(link).url
    except :
        return 0

    # create or update domain data
    domain = url.domain(link)
    res = es.index(index="web",id=domain, body={
        "homepage":link,
        "domain":domain,
        "email":email
    })

    return 1

@app.route("/search", methods=['POST'])
def search():
    """
    URL : /search
    Query engine to find a list of relevant URLs.
    Method : POST
    Form data :
        - query : the search query [string, required]
        - hits : the number of hits returned by query [integer, optional, default:10]
        - start : the start of hits [integer, optional, default:0]
    Return a sublist of matching URLs sorted by relevance, and the total of matching URLs.
    """
    def format_result(hit, highlight) :
        # highlight title and description
        title = hit["title"]
        description = hit["description"]
        if highlight :
            if "description" in highlight :
                description = highlight["description"][0]+"..."
            elif "body" in highlight :
                description = highlight["body"][0]+"..."
            """if "title" in highlight :
                title = highlight["title"][0]"""

        # create false title and description for better user experience
        if not title :
            title = hit["domain"]
        if not description :
            description = url.create_description(hit["body"])+"..."

        return {
            "title":title,
            "description":description,
            "url":hit["url"],
            "thumbnail":hit.get("thumbnail", None)
        }

    # get POST data
    data = dict((key, request.form.get(key)) for key in request.form.keys())
    logging.info("[Tiger] search request data : "+ str(data))
    if "query" not in data :
        raise InvalidUsage('No query specified in POST data')
    start = int(data.get("start", "0"))
    hits = int(data.get("hits", "10"))
    if start < 0 or hits < 0 :
        raise InvalidUsage('Start or hits cannot be negative numbers')

    # analyze user query
    groups = re.search("(site:(?P<domain>[^ ]+))?( ?(?P<query>.*))?",data["query"]).groupdict()
    if groups.get("query", False) and groups.get("domain", False) :
        # expression in domain query
        logging.info("[Tiger] Domain expression query")
        response = es.search(index="web-*", body=query.domain_expression_query(groups["domain"], groups["query"]), from_=start, size=hits)
        results = [format_result(hit["_source"], hit.get("highlight", None)) for hit in response["hits"]["hits"]]
        total = response["hits"]["total"]

    elif groups.get("domain", False) :
        # domain query
        logging.info("[Tiger] Domain query")
        response = es.search(index="web-*",body=query.domain_query(groups["domain"]), from_=start, size=hits)
        results = [format_result(hit["_source"], None) for hit in response["hits"]["hits"]]
        total = response["hits"]["total"]

    elif groups.get("query", False) :
        # expression query
        logging.info("[Tiger] Expression query : " + str(groups["query"]))
        response = es.search(index="web-en",body=query.expression_query(groups["query"]))
        logging.info("[Tiger] Raw response" + str(response))
        results = []
        for domain_bucket in response['aggregations']['per_domain']['buckets']:
            for hit in domain_bucket["top_results"]["hits"]["hits"] :
                results.append((format_result(hit["_source"], hit.get("highlight", None)),hit["_score"]))
        logging.info("[Tiger] Before Sort Results :" + str(results))        
        results = [result[0] for result in sorted(results, key=lambda result: result[1], reverse=True)]
        logging.info("[Tiger] After Sort Results :" + str(results))
        total = len(results)
        results = results[start:start+hits]
        logging.info("[Tiger] Total results : "+ str(total))
    return jsonify(total=total, results=results)
