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


# Define a function to handle database deletion
def delete_from_db(srn):
    result = db.students.delete_one({'srn': srn})
    if result.deleted_count > 0:
        print("Deleted student record with SRN:", srn)
    else:
        print("Student record with SRN", srn, "not found.")


# Define a callback function to handle incoming messages
def callback(ch, method, properties, body):
    srn = json.loads(body.decode('utf-8'))['srn']
    delete_from_db(srn)


# Consume messages from the queue
channel.basic_consume(queue='students', on_message_callback=callback, auto_ack=True)

print("Waiting for messages...")

# Start consuming messages
channel.start_consuming()