# Cloud-Computing-DBaaS-AMQP
A fault tolerant, highly available database as a service for the cloud based RideShare application was developed on AWS instances. The RideShare application allows the users to create a new ride if they are travelling from point A to point B and also pool rides. This is achieved with the help of REST APIs using Flask. We use two microservices, each for Users and Rides, started in separate docker containers running on one AWS instance. We also create an AWS Application Load Balancer which will distribute incoming HTTP requests to one of the two microservices based on the URL route of the request. Instead of using separate databases from each microservice, we implemented a DBaaS service' for the application. We created a custom database orchestrator engine that will listen to incoming HTTP requests from users and ride microservices and perform the database read and write operations.
  # Assignment 1:
	  Database used - sqlite3
	  Set up nginx,gunicorn and supervisor
	  Command to be run - python app1.py

  # Assignment 2:
	  Set up docker
	  Command to be run - sudo docker-compose up --build

  # Assignment 3:
	  Set up docker
	  Instances - User and Rides
	  Commands to be run on each instance:
		- sudo docker built -t imagename .
		- sudo docker run -p 80:5000 imagename
	  Loadbalancer - to distribute traffic

  # Project:
	  Set up docker
	  Instances - User, Rides, Orchestrator
	  Commands to be run on Users and Rides:
		- sudo docker built -t imagename .
		- sudo docker run -p 80:5000 imagename
	  Commands to be run on Orchestrator:
		- sudo docker compose up --build --scale worker=2
