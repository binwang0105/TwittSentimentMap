import os
import time
import json
import boto3
from codecs import open
from alchemyapi import AlchemyAPI
from create import create_topic

alchemyapi = AlchemyAPI()
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='TwitterSentimentMap')
topic = create_topic()

if __name__ == '__main__':
    while True:
        for message in queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=20):
            try:
                tweet = json.loads(message.body)
                response = alchemyapi.sentiment('text', tweet['text'])
                if response['status'] == 'OK':
                    tweet['sentiment'] = response['docSentiment']['type']
                    encoded = json.dumps(tweet, ensure_ascii=False)
                    topic.publish(Message=encoded)
                else:
                    print "text analysis error"
            except Exception as e:
                print "other error"
            finally:
                message.delete()
