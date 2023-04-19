import pika
import os
import pymongo

mongo_host = os.environ.get("MONGO_HOST", "localhost")
mongo_port = int(os.environ.get("MONGO_PORT", "27017"))

mongo_client = pymongo.MongoClient(host=mongo_host, port=mongo_port)
mongo_db = mongo_client["Student"]
mongo_collection = mongo_db["students"]

def callback(ch, method, properties, body):
    srn = body.decode('utf-8')
    student = mongo_collection.find_one({'SRN': srn})
    if student is None:
        print("Student with SRN {} not found".format(srn))
    else:
        print("Student Details:")
        print("SRN: ", student['SRN'])
        print("Name: ", student['Name'])
        print("Program: ", student['Program'])
        print("Email: ", student['Email'])

def main():
    rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
    rabbitmq_port = int(os.environ.get("RABBITMQ_PORT", "5672"))
    rabbitmq_username = os.environ.get("RABBITMQ_USERNAME", "guest")
    rabbitmq_password = os.environ.get("RABBITMQ_PASSWORD", "guest")

    credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
    parameters = pika.ConnectionParameters(rabbitmq_host,
                                           rabbitmq_port,
                                           '/',
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='read')

    channel.basic_consume(queue='read', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    main()