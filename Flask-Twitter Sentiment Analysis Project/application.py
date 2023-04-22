from flask import Flask, render_template, request,jsonify,url_for
import os
from models.db import get_tweets_by_country,get_topics_retweets,get_tweets_aggregation,get_tweets_by_keyword
import pandas as pd
from json import loads, dumps

application = Flask(__name__)

@application.route('/search/results', methods=['GET', 'POST'])
def request_search():
    search_term = request.form["input"]
    tweets=get_tweets_by_keyword(search_term)
    return render_template('results.html',x=tweets)

@application.route('/', methods=['GET', 'POST'])
def home():
    return render_template('home.html')


@application.route('/charts', methods=['GET', 'POST'])
def charts_view():

    retweet_lst=get_topics_retweets()
    retweet_df = pd.DataFrame(retweet_lst)

    retweet_dct = dict()
    retweet_dct['topic'] = 'retweet_count'


    for index, row in retweet_df.iterrows():
        key = row['_id']
        value = row['total_retweets']
        retweet_dct[key] = value


    lst = get_tweets_aggregation()
    df = pd.DataFrame(lst)

    x = dict()
    x['topic']= 'count'


    for index, row in df.iterrows():
        key=row['_id']
        value= row['total']
        x[key]=value

    return render_template('viz.html',data= x,bar_data=retweet_dct)



@application.route('/countries', methods=['GET'])
def get_tweets_by_countries():
    try:
        countries = request.args.getlist('countries')
        results = get_tweets_by_country(countries)
        response_object = {
            "titles": results
        }
        return jsonify(response_object), 200
    except Exception as e:
        response_object = {
            "error": str(e)
        }
        return jsonify(response_object), 400



if __name__ == '__main__':
    application.config['MONGO_URI'] = "mongodb+srv://twitterapiuser:tiet2009@cluster0.yy5azz6.mongodb.net/sample_twitter?retryWrites=true&w=majority"
    application.run('127.0.0.1', debug=True)