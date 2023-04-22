import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import requests as requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://twitterapiuser:tiet2009@cluster0.yy5azz6.mongodb.net/?retryWrites=true&w=majority"


# Create a new client and connect to the server


class TwitterClient(object):

    def __init__(self):
        '''
        Class constructor or initialization method.
        '''
        consumer_key = 'C66qOQGhuq6GgnW0ISBgg5dAf'
        consumer_secret = 'PHvrVLmhkbbD1NwQWWjGH3KpLJO0zjzXwGBmU5Y6ySwBUOmOER'
        access_token = '1647392337788559361-6J7Y9w5Pv4JrgJpVAoO2ct4WTQumEL'
        access_token_secret = 'y92QBYv92mKH2dIeZ8ZdPB0Ht2OdmbmgCuNiSkr5L7l81'

        # attempt authentication
        try:
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            self.auth.set_access_token(access_token, access_token_secret)
            self.api = tweepy.API(self.auth)
        except:
            print("Error: Authentication Failed")

    def clean_tweet(self, tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_tweet_sentiment(self, tweet):
        '''
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        '''

        analysis = TextBlob(self.clean_tweet(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'

    def get_tweets(self, query, count):
        '''
        Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = []

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search_tweets(q=query, count=count)
            print("Fetched tweets", fetched_tweets)

            # parsing tweets one by one
            for tweet in fetched_tweets:
                parsed_tweet = {}

                # saving text of tweet
                parsed_tweet['tweet'] = tweet.text
                # saving sentiment of tweet
                parsed_tweet['sentiment'] = self.get_tweet_sentiment(tweet.text)
                parsed_tweet['topic'] = query
                parsed_tweet['time'] = tweet.created_at
                parsed_tweet['user'] = tweet.user.screen_name
                # parsed_tweet['tweet']=tweet.full_text
                parsed_tweet['location'] = tweet.user.location
                parsed_tweet['description'] = tweet.user.description
                parsed_tweet['followers'] = tweet.user.followers_count
                parsed_tweet['retweet_count'] = tweet.retweet_count

                # appending parsed tweet to tweets list
                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet not in tweets:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)

            # return parsed tweets
            return tweets

        except tweepy.TweepError as e:
            # print error (if any)
            print("Error : " + str(e))


def main():
    # creating object of TwitterClient Class
    api = TwitterClient()
    # calling function to get tweets
    tweets = api.get_tweets(query='tesla', count=1)
    print("Tweets List", type(tweets), tweets)

    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client["sample_twitter"]
    db.tweet_collection.insert_many(tweets)


if __name__ == "__main__":
    # calling main function
    main()
