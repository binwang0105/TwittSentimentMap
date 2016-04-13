from credentials import endpoint
import boto3

sns = boto3.resource('sns')

def create_topic():
    topic = sns.create_topic(Name='TwitterSentimentMap')
    return topic

def subscribe(topic):
    topic.subscribe(Protocol='http', Endpoint=endpoint)

if __name__ == '__main__':
    topic = create_topic()
    subscribe(topic)
