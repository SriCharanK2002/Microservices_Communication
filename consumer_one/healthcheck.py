import pika

# Establish a connection with RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))

print("RabbitMQ connection established successfully.")