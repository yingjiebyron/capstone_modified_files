import time
import aylien_news_api
from aylien_news_api.rest import ApiException
from pprint import pprint
import pandas as pd
import json
import pandas as pd
import dns
import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np
import re

class aylien_news:
    def __init__(self, db):
        self.db = db
        self.news_collection = db['News']
        self.activity_collection = db.Activity
    def config(self, Aylien_ID, Aylien_key):
        configuration = aylien_news_api.Configuration()
        client = aylien_news_api.ApiClient(configuration)
        configuration.api_key['X-AYLIEN-NewsAPI-Application-ID'] = Aylien_ID
        configuration.api_key['X-AYLIEN-NewsAPI-Application-Key'] = Aylien_key
        self.api_instance = aylien_news_api.DefaultApi(client)
    def entity_link(self,company_name):
        #company_shorter_name = company_gfcid_name.replace(' Pte Ltd','').replace(' Ltd','').replace(' Limited','').replace(' Holdings','')
        opts = {
        'type': 'dbpedia_resources',
        'term': company_name, #'Sheng Siong',
        'language': 'en',
        'per_page': 3
        }
        try:
            # List autocompletes
            autocomplete = self.api_instance.list_autocompletes(**opts)
            autocomplete_ls = autocomplete.autocompletes
            entity_link = autocomplete_ls[0].id
            #pprint(autocomplete)
        except IndexError:
            entity_link = 'null'
        except ApiException as e:
            print("Exception when calling DefaultApi->list_autocompletes: %s\n" % e)
            entity_link = 'null'
        return entity_link
    def call_stories_endpoint(self,params):
        try:
            api_response = self.api_instance.list_stories(**params)
            return api_response.stories
        except ApiException as e:
            print("Exception when calling DefaultApi->list_stories: %sn" % e)
            return None
    def extract_news(self, company_gfcid_name,per_page = 10):
        company_shorter_name = company_gfcid_name.replace(' Pte Ltd','').replace(' Ltd','').replace(' Limited','').replace(' Holdings','')
        entity_title_links_dbpedia = self.entity_link(company_shorter_name)
        if entity_title_links_dbpedia != 'null':
            #print(enti)
            params = {
            'sort_by': 'source.rankings.alexa.rank.SG',
            'language': ['en'],
            'published_at_start': 'NOW-1DAYS',
            'published_at_end': 'NOW',
            'entities_title_links_dbpedia': [entity_title_links_dbpedia],
            'entities_title_type': ['Orgainzation','Agent','Company'],
            'per_page':per_page
            }
            stories = self.call_stories_endpoint(params)

            if len(stories) == 0:
                #print('{} no news by entity'.format(company_shorter_name))
                title_keywords = company_shorter_name.replace(' ',' AND ')
                params = {
                'sort_by': 'source.rankings.alexa.rank.SG',
                'language': ['en'],
                'published_at_start': 'NOW-1DAYS',
                'published_at_end': 'NOW',
                'entities_title_type': ['Orgainzation','Agent','Company'],
                'title':title_keywords,
                'per_page':per_page
                }
                stories = self.call_stories_endpoint(params)
                #print(stories)
        else:
            #print('{} has no entity link'.format(company_shorter_name))
            title_keywords = company_shorter_name.replace(' ',' AND ')
            params = {
            'sort_by': 'source.rankings.alexa.rank.SG',
            'language': ['en'],
            'published_at_start': 'NOW-1DAYS',
            'published_at_end': 'NOW',
            'entities_title_type': ['Orgainzation','Agent','Company'],
            'title':title_keywords,
            'per_page':per_page
            }
            stories = self.call_stories_endpoint(params)

        return stories
    
    def clean(self, data):
        #cleanr = re.compile("<.*?>")
        cleanr= re.sub(r"http\S+", "", data)
        #cleanr= re.sub(cleanr, "", data)
        #cleantext = re.sub(cleanr,'',data)
        cleantext = re.sub('[^a-zA-Z# ]','',cleanr)
        clean_hash = re.sub(r"#\S+","",cleantext)

        return clean_hash

    def sentiment(self, summary):
        sid = SentimentIntensityAnalyzer()
        cleantext= self.clean(summary)
        score= sid.polarity_scores(cleantext)
        return (score["compound"])

    def summary_para (self,summary_ls):
        #transform list of sentences in summary to a string
        summary = ''
        for sentence in summary_ls:
            summary+=sentence
        return summary

    def create_news_dict(self,story,gfcid,company_GFCID_name):
        unix_time = int(time.mktime(datetime.datetime.strptime(str(story.published_at.date()), '%Y-%m-%d').timetuple()))
        news_dict = {
            #"clusterID":story.clusters,
            'gfcid':gfcid,
            'newsID':str(story.id),
            'company':company_GFCID_name,
            'source':story.source.name,
            'date':unix_time,
            'title': story.title,
            'url': story.links.permalink,
            'summary': self.summary_para(story.summary.sentences),
            'sentiment':story.sentiment.to_dict(),
            'locEntity':self.get_entity(story.entities.body,'Location'),
            'orgEntity':self.get_entity(story.entities.body,'Company'),
            'sentiment':self.sentiment(self.summary_para(story.summary.sentences)),
            'body':story.body
                    }
        return news_dict

    def into_clusters(self,company_GFCID_name,gfcid,no_of_news = 20):
        #get news of the company
        stories = self.extract_news(company_GFCID_name,no_of_news)

        clustered_stories = {}
        clusters = []
        no_cluster = 1
        news_of_company = []
        if stories:
            for story in stories:
                if len(story.clusters) > 0:
                    cluster = story.clusters[0]
                    if cluster not in clusters:
                        clustered_stories[cluster] = [story.title]
                        news_detail = self.create_news_dict(story,gfcid,company_GFCID_name)
                        clusters.append(cluster)
                        news_of_company.append(news_detail)
                    else:
                        clustered_stories[cluster].append(story.title)
                        #print(story.id)
                else:
                    #print("no cluster")
                    clustered_stories[no_cluster] = [story.title]
                    no_cluster+=1
                    clusters.append(no_cluster)
                    news_detail = self.create_news_dict(story,gfcid,company_GFCID_name)
                    #data[company_GFCID_name].append(news_detail)
                    news_of_company.append(news_detail)
        return news_of_company
    
    def get_entity (self,news_entity, entity_type):
        """news_entity = story.entities.body"""
        entity_ls = []
        for entity in news_entity:
            entity = entity.to_dict()
            if entity_type in entity['types']:
                entity_ls.append(entity['text'])
        return entity_ls

    def insert_news(self, gfcid = None):
        #companies_collection : list,  retrieved company data from Mongodb
        db = self.db
        if not gfcid:
            cursor_company = db['Company'].find({},{'company':1,'industry':1,'gfcid':1,'industryID':1,'_id':0})
            company_collection = list(cursor_company)
        else:
            cursor_company = db['Company'].find({"gfcid":gfcid},{'company':1,'industry':1,'gfcid':1,'industryID':1,'_id':0})
            company_collection = list(cursor_company)
        for company_detail in company_collection:
            #document = []
            company_GFCID_name = company_detail['company']
            #print(company_GFCID_name)
            gfcid = company_detail['gfcid']
            all_news = self.into_clusters(company_GFCID_name=company_GFCID_name,gfcid=gfcid,no_of_news=20) #return list of dictionay of news
            time.sleep(2)
            if len(all_news) >0:
                try:
                    self.news_collection.insert_many(all_news)
                    today = datetime.datetime.now().strftime('%Y-%m-%d')
                    self.activity_collection.insert_one({"gfcid":gfcid, "date":today,"type":"company news","count":len(all_news)})
                    print("successfully loaded the data for {}".format(company_GFCID_name))

                except Exception as e:
                    print("Mongo insertion failed: ", e)
            else:
                print('{} has no news in the past 1 day'.format(company_GFCID_name))
