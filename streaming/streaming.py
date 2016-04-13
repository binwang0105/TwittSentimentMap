import os
import json
import time
import tweepy
import boto3
from codecs import open
from dateutil import parser
from credentials import consumer_key, consumer_secret, access_token, access_token_secret

sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='TwitterSentimentMap')

class TwittMapListener(tweepy.StreamListener):
    def __init__(self):
        super(TwittMapListener, self).__init__()

    def on_error(self, status):
        if status == 420:
            return False

    def on_data(self, data):
        try:
            decoded = json.loads(data)
            if decoded.get('lang') == 'en' and decoded.get('coordinates') is not None:
                geo = decoded['coordinates']['coordinates']
                timestamp = parser.parse(decoded['created_at']).strftime('%Y-%m-%dT%H:%M:%SZ')
                tweet = {
                    'user': decoded['user']['screen_name'],
                    'text': decoded['text'],
                    'geo': geo,
                    'time': timestamp
                }
                encoded = json.dumps(tweet, ensure_ascii=False)
                queue.send_message(MessageBody=encoded)
        except Exception as e:
            print "message loading error"

if __name__ == '__main__':

        l = TwittMapListener()
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = tweepy.Stream(auth, l)
        stream.filter(track=["Trump", "Curry", "Obama", "Facebook", "Car",
                             "New York", "Amazon", "Columbia", "America", "Test"])
