Question:
	1. What to do when primary fails before all clients start up
	2. How does the client connect to the primary
	3. Is primary both proposer and accepter and learner
	4. Why do we have multiple learners
	5. Will there be multiple proposers in our project
	6. How are we going to detect crash?
	7. Do all clients send to the same replica or all replicas


Client receive:
	1. Update primary
	2. Initialization
	3. Update message
	4. Send response

Client send:
    1. Initialization step
    2. New message
    3. Termination

Server send:
	1. Update primary
	2. Initialization
	3. Update message
	4. Send response

Server receive:
    1. Initialization step
    2. New message
    3. Termination


Steps:
	1. Servers start up
	2. All clients send initialization
	3. Each client sends a message, wait for send response, and send again