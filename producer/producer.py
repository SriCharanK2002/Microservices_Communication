import pika
import json
from pymongo import MongoClient

# Connect to the MongoDB database
client = MongoClient('mongodb://mongodb:27017/')
db = client['Student']

# Establish a connection with RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))

channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='students')


# Define a function to handle database insertion
def insert_to_db(data):
    db.students.insert_one(data)
    print("Inserted student record:", data)


# Define a function to handle incoming messages
def callback(ch, method, properties, body):
    data = json.loads(body.decode('utf-8'))
    insert_to_db(data)


# Consume messages from the queue
channel.basic_consume(queue='students', on_message_callback=callback, auto_ack=True)

print("Waiting for messages...")

# Start consuming messages
channel.start_consuming()