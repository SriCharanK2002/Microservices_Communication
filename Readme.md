# Microservice communication with RabbitMQ

Project by: Kanumuri Sri Charan, Keerthana Shivakumar, Keerthi R and Mahika Gupta

## Problem Statement

Building and deploying a microservices architecture where multiple components communicate with each other using RabbitMQ. A message broker is an architectural pattern for message validation, transformation and routing. For the scope of this project, we will build 4 microservices: A HTTP server that handles incoming requests to perform CRUD operations on a Student Management Database + Check the health of the RabbitMQ connection, a microservice that acts as the health check endpoint, a microservice that inserts a single student record, a microservice that retrieves student records, a microservice that deletes a student record given the SRN.

## File Structure 

```bash
├── <microservices-project-directory>
    ├── docker-compose.yml
    ├── producer
    │   ├── producer.py
    │   ├── Dockerfile
        └──requirements.txt
    ├── consumer_one
    │   ├── healthcheck.py
    │   ├── Dockerfile
    │   └──requirements.txt
    ├── consumer_two
    │   ├── insertion.py
    │   ├── Dockerfile
    │   └──requirements.txt
    ├── consumer_three
    │   ├── deletion.py
    │   ├── Dockerfile
    │   └──requirements.txt
    └── consumer_four
        ├── read.py
        ├── Dockerfile
        └──requirements.txt

```

