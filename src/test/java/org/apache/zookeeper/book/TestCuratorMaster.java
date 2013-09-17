package org.apache.zookeeper.book;

import java.util.ArrayList;

import org.apache.curator.retry.ExponentialBackoffRetry;

import org.apache.zookeeper.book.Client.TaskObject;
import org.apache.zookeeper.book.Master.MasterStates;
import org.apache.zookeeper.book.curator.CuratorMasterSelector;
import org.apache.zookeeper.book.curator.CuratorMasterLatch;

import org.junit.Test;
import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCuratorMaster extends BaseTestCase {
private static final Logger LOG = LoggerFactory.getLogger(TestTaskAssignment.class);
    


    @Test(timeout=60000)
    public void testTaskAssignment() throws Exception {
        LOG.info("Starting master (taskAssignment)");
        CuratorMasterSelector m = new CuratorMasterSelector("M1", "localhost:" + port, 
                new ExponentialBackoffRetry(1000, 5));
        m.startZK();
        
        m.bootstrap();
        m.runForMaster();
        
        LOG.info("Going to wait for leadership");
        m.awaitLeadership();
        
        LOG.info("Starting worker");
        Worker w1 = new Worker("localhost:" + port);
        Worker w2 = new Worker("localhost:" + port);
        Worker w3 = new Worker("localhost:" + port);
        
        w1.startZK();
        w2.startZK();
        w3.startZK();
        
        while(!w1.isConnected() && !w2.isConnected() && !w3.isConnected()) {
            Thread.sleep(100);
        }   
        
        /*
         * bootstrap() create some necessary znodes.
         */
        w1.bootstrap();
        w2.bootstrap();
        w3.bootstrap();
        
        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w1.register();
        w2.register();
        w3.register();
        
        w1.getTasks();
        w2.getTasks();
        w3.getTasks();
        
        LOG.info("Starting client");
        Client c = new Client("localhost:" + port);
        c.startZK();
        
        while(!c.isConnected() && 
                !w1.isConnected() &&
                !w2.isConnected() &&
                !w3.isConnected()){
            Thread.sleep(100);
        }   
        
        TaskObject task = null;
        for(int i = 1; i < 200; i++) {
            task = new TaskObject();
            c.submitTask("Sample task taskAssignment " + i, task);
            task.waitUntilDone();
            Assert.assertTrue("Task not done", task.isDone());
        }
        
        w1.close();
        w2.close();
        w3.close();
        m.close();
    }

    @Test(timeout=60000)
    public void testZooKeeperRestart() throws Exception {
        LOG.info("Starting zookeeper restart");
        CuratorMasterSelector m = new CuratorMasterSelector("M1", "localhost:" + port, 
                new ExponentialBackoffRetry(1000, 5));
        m.startZK();
        m.bootstrap();
        
        LOG.info("Starting worker");
        Worker w1 = new Worker("localhost:" + port);
        Worker w2 = new Worker("localhost:" + port);
        Worker w3 = new Worker("localhost:" + port);
        
        w1.startZK();
        w2.startZK();
        w3.startZK();
        
        while(!w1.isConnected() && !w2.isConnected() && !w3.isConnected()) {
            Thread.sleep(100);
        }   
        
        /*
         * bootstrap() create some necessary znodes.
         */
        w1.bootstrap();
        w2.bootstrap();
        w3.bootstrap();
        
        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w1.register();
        w2.register();
        w3.register();
        
        w1.getTasks();
        w2.getTasks();
        w3.getTasks();
        
        LOG.info("Starting client");
        Client c = new Client("localhost:" + port);
        c.startZK();
        
        while(!c.isConnected() && 
                !w1.isConnected() &&
                !w2.isConnected() &&
                !w3.isConnected()){
            Thread.sleep(100);
        }   

        int numTasks = 200;
        TaskObject task = null;
        ArrayList<TaskObject> taskList = new ArrayList<TaskObject>(numTasks);
        for(int i = 0; i < numTasks; i++) {
            task = new TaskObject();
            c.submitTask("Sample task zkRestart " + i, task);
            taskList.add( task );
        }
        
        /*
         * Restart ZK server
         */
        LOG.info("Restarting ZooKeeper");
        restartServer();
        
        /*
         * Now try to get mastership
         */
        m.runForMaster();
        
        LOG.info("Going to wait for leadership");
        m.awaitLeadership();

        for(int i = 0; i < numTasks; i++){
            taskList.get( i ).waitUntilDone();
            Assert.assertTrue("Task not done", taskList.get( i ).isDone());
        }
        
        /*
         * Wrapping up
         */
        w1.close();
        w2.close();
        w3.close();
        m.close();
    }
    
    @Test(timeout=30000)
    public void electSingleMaster() 
    throws Exception {

        LOG.info( "Starting single master test" );
        CuratorMasterSelector m = new CuratorMasterSelector("M1", "localhost:" + port, 
                new ExponentialBackoffRetry(1000, 5));
        CuratorMasterSelector bm = new CuratorMasterSelector("M2", "localhost:" + port, 
                new ExponentialBackoffRetry(1000, 5));
        
        LOG.info("Starting ZooKeeper for M1");
        m.startZK();
        
        LOG.info("Starting ZooKeeper for M2");
        bm.startZK();
        
        m.bootstrap();
        //bm.bootstrap();
        
        bm.runForMaster();
        m.runForMaster();
        
        while(!m.isLeader() && !bm.isLeader()){
            LOG.info( "m: " + m.isLeader() + ", bm: " + bm.isLeader() );
            Thread.sleep(100);
        }
        
        boolean singleMaster = ((m.isLeader() && !bm.isLeader()) 
                || (!m.isLeader() && bm.isLeader()));
        Assert.assertTrue("Master not elected.", singleMaster);
        m.close();
    }
    
    @Test(timeout=60000)
    public void testTaskAssignmentLatch() throws Exception {
        LOG.info("Starting master (taskAssignment)");
        CuratorMasterLatch m = new CuratorMasterLatch("M1", "localhost:" + port, 
                new ExponentialBackoffRetry(1000, 5));
        m.startZK();
        
        m.bootstrap();
        m.runForMaster();
        
        LOG.info("Going to wait for leadership");
        m.awaitLeadership();
        
        LOG.info("Starting worker");
        Worker w1 = new Worker("localhost:" + port);
        Worker w2 = new Worker("localhost:" + port);
        Worker w3 = new Worker("localhost:" + port);
        
        w1.startZK();
        w2.startZK();
        w3.startZK();
        
        while(!w1.isConnected() && !w2.isConnected() && !w3.isConnected()) {
            Thread.sleep(100);
        }   
        
        /*
         * bootstrap() create some necessary znodes.
         */
        w1.bootstrap();
        w2.bootstrap();
        w3.bootstrap();
        
        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w1.register();
        w2.register();
        w3.register();
        
        w1.getTasks();
        w2.getTasks();
        w3.getTasks();
        
        LOG.info("Starting client");
        Client c = new Client("localhost:" + port);
        c.startZK();
        
        while(!c.isConnected() && 
                !w1.isConnected() &&
                !w2.isConnected() &&
                !w3.isConnected()){
            Thread.sleep(100);
        }   
        
        TaskObject task = null;
        for(int i = 1; i < 200; i++) {
            task = new TaskObject();
            c.submitTask("Sample task taskAssignment " + i, task);
            task.waitUntilDone();
            Assert.assertTrue("Task not done", task.isDone());
        }
        
        w1.close();
        w2.close();
        w3.close();
        m.close();
    }
}
