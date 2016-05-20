# Apache ZooKeeper - O'Reilly Book Example

This is some code example we have developed for the Apache ZooKeeper book. 
This code constitutes complementary material to the book and it has been 
written to illustrate how to implement an application with ZooKeeper. It
hasn't been heavily tested or debugged, and it misses features, so don't 
take it as production-ready code. In fact, if you're able to fix bugs and
extend this implementation, it probably means that you have learned how
to program with ZooKeeper!


## Components

This example implements a simple master-worker system. There is a primary
master assigning tasks and it supports backup masters to replace the primary
in the case it crashes. Workers execute tasks assigned to it. The task
consists of reading the content of the task znode, nothing more. A real app
will most likely do something more complex than that. Finally, clients 
submit tasks and wait for a status znode.

Here is a summary of the code flow for each of the components:

### Master:

  1. Before taking leadership 
    1. Try to create the master znode
    2. If it goes through, then take leadership
    3. Upon connection loss, needs to check if znode is there and who owns it
    4. Upon determining that someone else owns it, watch the master znode
  2. After taking leadership 
    1. Get workers
      1. Set a watch on the list of workers
      2. Check for dead workers and reassign tasks
      3. For each dead worker
        1. Get assigned tasks
        2. Get task data
        3. Move task to the list of unassigned tasks
        4. Delete assignment
    2. Recover tasks (tasks assigned to dead workers)
    3. Get unassigned tasks and assign them
    4. For each unassigned task
    5. 
      1. Get task data
      2. Choose worker
      3. Assign to worker
      4. Delete task from the list of unassigned

### Worker:


  1. Creates /assign/worker-xxx znode
  2. Creates /workers/worker-xxx znode
  3. Watches /assign/worker-xxx znode
  4. Get tasks upon assignment
  5. For each task, get task data
  6. Execute task data
  7. Create status
  8. Delete assignment


### Client


  1. Create task
  2. Watch for status znode
  3. Upon receiving a notification for the status znode, get status data
  4. Delete status znode 


## Compile and run it

We used maven for this little project. Install maven if you don't have it
and run "mvn install" to generate the jar file and to run tests. We have a
few tests that check for basic functionality. For the C master, you'll need
to compile the ZooKeeper C client to use the client library.

To run it, follow these steps:

### Step 1: Start ZooKeeper by running `bin/zkServer.sh start` from a copy 
of the distribution package.

### Step 2: Start the master
```
java -cp .:/usr/local/zookeeper-3.4.8/zookeeper-3.4.8.jar:/usr/local/slf4j-1.7.2/slf4j-api-1.7.2.jar:/usr/local/slf4j-1.7.2/slf4j-ext-1.7.2.jar:/usr/local/slf4j-1.7.2/slf4j-log4j12-1.7.2.jar:/usr/local/apache-log4j-1.2.17/log4j-1.2.17.jar:/path/to/book/repo/target/ZooKeeper-Book-0.0.1-SNAPSHOT.jar org.apache.zookeeper.book.Master localhost:2181
```

### Step 3: Start a couple of workers
```
java -cp .:/usr/local/zookeeper-3.4.8/zookeeper-3.4.8.jar:/usr/local/slf4j-1.7.2/slf4j-api-1.7.2.jar:/usr/local/slf4j-1.7.2/slf4j-ext-1.7.2.jar:/usr/local/slf4j-1.7.2/slf4j-log4j12-1.7.2.jar:/usr/local/apache-log4j-1.2.17/log4j-1.2.17.jar:/path/to/book/repo/target/ZooKeeper-Book-0.0.1-SNAPSHOT.jar org.apache.zookeeper.book.Worker localhost:2181
```

### Step 4: Run a client
```
java -cp .:/usr/local/zookeeper-3.4.8/zookeeper-3.4.8.jar:/usr/local/slf4j-1.7.2/slf4j-api-1.7.2.jar:/usr/local/slf4j-1.7.2/slf4j-ext-1.7.2.jar:/usr/local/slf4j-1.7.2/slf4j-log4j12-1.7.2.jar:/usr/local/apache-log4j-1.2.17/log4j-1.2.17.jar:/path/to/book/repo/target/ZooKeeper-Book-0.0.1-SNAPSHOT.jar org.apache.zookeeper.book.Client localhost:2181
```

For the C master, we do the following:

### Compile

```
gcc -I/usr/local/zookeeper-3.4.8/src/c/include -I/usr/local/zookeeper-3.4.8/src/c/generated -DTHREADED -L/usr/local/lib -l zookeeper_mt master.c
```
### Run it

./a.out 127.0.0.1:2181

Have fun!
