Ongarbekov Ilyas

1--- Introduction

In this lab, I created a simple distributed counter system using Python and gRPC.

The main idea was to understand how different servers communicate with each other and how a system works when one of the servers is unavailable.

I also worked with retries, duplicate requests, Lamport clocks, replication and recovery.

2--- Technologies Used

For this lab, I used: Python for writing the server and client, gRPC for communication between the client and server, Protocol Buffers for defining requests and responses, Threading for handling multiple requests, Pytest for testing my code

I used VS Code to write and run the project.

3--- How My System Works

My system has one client and three server replicas.

The servers use these ports: Server 1 50051, Server 2 50052, Server 3 50053

The client can send requests to increase a counter or get its current value.

Each server stores its own counter values in memory.

I also created a replication manager that sends increment requests to all three servers.

4--- Main Features

gRPC Counter

First, I created a counter service with two main methods: Increment and Get.

Increment increases the counter value, and Get returns the current value.

Idempotency

I used an idempotency key to prevent the same request from increasing the counter twice.

For example, if a client sends the same request again, the server recognizes it and returns the previous result.

Retries

I added retries to the client. If a request fails because of a timeout or connection problem, the client can try again.

Concurrency

I used threading and a lock to make sure multiple requests do not change the counter at the same time incorrectly.

Lamport Clocks

I implemented Lamport logical clocks to track the order of events.

The clock is updated when requests are received and when events happen on the server.

5--- Replication and Quorum

In the second part, I worked with three replicas.

The replication manager sends requests to the servers and checks how many of them respond successfully.

For three replicas, the majority quorum is two.


6--- Replica Recovery

I also implemented a basic recovery function.

For example, if one server has a counter value of 30 and the other servers have lost their data, I can copy the value from the first server to the others.

I used the Restore method for this.

After running the recovery test, I got these results:

Replica 50051: 30
Replica 50052: 30
Replica 50053: 30

The program showed:

ALL REPLICAS CONSISTENT
RECOVERY TEST PASSED

This means all three replicas had the same counter value after synchronization.

7-- Testing

I used Pytest to check if my code works correctly.

I tested counter increments, duplicate requests, concurrent requests, retries and server failures.

After running the tests, I got:

8 passed in 0.80s

I also ran a separate recovery test, and it passed.
8--- Problems and Limitations

During this lab, I noticed that the servers lose their counter values when I restart them because the data is stored only in memory.

That is why I added a basic recovery function.

However, the recovery process is not fully automatic. I need to choose which server has the correct value.

Also, my system does not have a database or advanced conflict handling between replicas.

9--- Conclusion

In this lab, I learned how to create a simple distributed system using Python and gRPC.

Before this lab, I mostly worked with programs running as one process. Here, I worked with multiple servers and tested how they communicate.

The most interesting parts for me were replication and recovery because I could see what happens when some servers stop working or lose their data.

In the end, I managed to run three replicas, test different failure situations and synchronize their counter values.

The system is still simple, but it helped me understand the basic ideas of distributed systems.