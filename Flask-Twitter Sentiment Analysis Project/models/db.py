"""
This module contains all database interfacing methods for the Twitter
application.

Look out for TODO markers for additional help. Good luck!
"""

from flask import current_app, g
from werkzeug.local import LocalProxy
from flask_pymongo import PyMongo
from pymongo.errors import DuplicateKeyError, OperationFailure
from bson.objectid import ObjectId
from bson.errors import InvalidId
import pandas as pd


def get_db():
    """
    Configuration method to return db instance
    """
    db = getattr(g, "_database", None)

    if db is None:
        db = g._database = PyMongo(current_app).db

    return db


# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)


def get_tweets_by_country(countries):
    """
    Finds and returns tweets by country.
    Returns a list of dictionaries, each dictionary contains a title and an _id.
    """
    try:
        print(f" c: {countries}")
        return list(db.tweet_collection.find({}, {"country": 1}))

    except Exception as e:
        return e

def get_tweets_by_keyword(search_word):

    try:

        print(f" c: {search_word}")


        tweet=db.tweet_collection.find({'tweet': {'$regex': search_word, '$options': 'i'}})
        df = pd.DataFrame(list(tweet))
        return df


    except Exception as e:
        return e

def get_tweets():

    try:

        tweet=db.tweet_collection.find()
        df = pd.DataFrame(list(tweet))
        return df

    except Exception as e:
        return e

def get_tweets_aggregation():

    try:
        agg_result = db.tweet_collection.aggregate(
            [{
                "$group":
                    {"_id": "$topic",
                     "total": {"$sum": 1}
                     }}
            ])
        return list(agg_result)

    except Exception as e:
        return e

def get_tweets_by_user(username):

    try:

        pipeline = [
            {
                "$match": {
                    "user": username
                }
            }
        ]

        tweets = db.tweet_collection.aggregate(pipeline).next()
        return list(tweets)

    # TODO: Error Handling
    except (StopIteration) as _:

        return None

    except Exception as e:
        return {}


def get_topics_retweets():
    try:
        agg_result = db.tweet_collection.aggregate(
            [{
                "$group":
                    {"_id": "$topic",
                     "total_retweets": {"$sum": "$retweet_count"}
                     }}
            ])
        return list(agg_result)

    # TODO: Error Handling
    except (StopIteration) as _:
        return None

    except Exception as e:
        return {}

