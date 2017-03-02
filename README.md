TODO:
    0. Can not connect to CAEN computer, connection refused
    1. Set up simple client server communication with chat log. Need ClientID, clientSeqNum, and message
    2. Implement more logic

Question:
	1. What to do when primary fails before all clients start up
	2. How does the client connect to the primary
	3. Is primary both proposer and accepter and learner
	4. Why do we have multiple learners
	5. Will there be multiple proposers in our project
	6. How are we going to detect crash?
	7. Do all clients send to the same replica or all replicas
	8. after getting followers, do we broadcast to everyone or just followers

Messages
    Client to Server
    1. Client sends header with info about msg size {clientId, clientSeqNum, msgSize, "$"}
    2. Client sends chat message after header {clientId, clientSeqNum, chatMsg}

Header Message
    1. C | id | seq num | $  message size   =>      Client Send Header
   
    2. L | viewNum | $ =>  I am Leader message.          Proposer sends to acceptors. ONLY NEED HEADER MESSAGE here
    3. F | viewNum | value | prevView# | $ => You are leader message. Acceptor sends to proposer
    4. P | viewNum | value | $ => Leader proposes value. Proposer sends to majority
    5. A | viewNum | value | $ => Follower accepts value. Acceptor sends to learner

Client receive:
	1. Send response

Client send:
    1. Chat message
    2. Broadcast message

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

class Server{
	int current_primary = 0;
	receive_request(){
		if primary:
			if not_already_excuted:
				add to queue
			else:
				respond with executed
		else:
			if primary_alive(current_primary):
				do_nothing
			else:
				re_elect()
	}

	service_request(){
		if queue.not_empty():
			pop_from_queue()
			if not_already_excuted:
				then execute
			else:
				respond with executed
	}
};

class Client{
	send_request(){
		while(input()) {
			message = new_message(seq++)
			receive_num = current_primary;
			send_to_primary(message)
			while (time_out(receive_num)) {
				receive_num = 0
				broadcast(message)
			}
		}
	}

	time_out(int receive_num){
		if receive_num == 0:
			receive_from_everyone()
		else:
			receive_from(receive_num)
	}
};
