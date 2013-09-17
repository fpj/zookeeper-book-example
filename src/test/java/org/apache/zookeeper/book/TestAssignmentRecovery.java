/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package org.apache.zookeeper.book;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback;
import org.junit.Test;
import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAssignmentRecovery extends BaseTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestTaskAssignment.class);

    boolean connected = false;
    boolean recoveryDone = false;
    int status = RecoveryCallback.FAILED;
    List<String> recoveredTasks;
    
    @Test(timeout=50000)
    public void testRecovery(){
        try{
            ZooKeeper zk = new ZooKeeper("localhost:" + port, 10000, new Watcher(){
                public void process(WatchedEvent e) {
                    if(e.getState() == KeeperState.SyncConnected){
                        connected = true;
                    }
                    LOG.info("Event: " + e.toString());
                }
            });
            
            while(!connected){
                Thread.sleep(100);
            }
        
            /*
             * The number of recovered tasks should be 2 because
             * there is a single active worker, one task has been
             * assigned to an absent worker, and one task hasn't
             * been assigned at all. The last two need to be 
             * assigned, and consequently they are part of the
             * list of recovered tasks.
             *
             * Note that recovery here refers to tasks that a new
             * master needs to reassign when failing over. It is
             * not related to the recovery of the tasks themselves
             * as it could happen if a worker crashes before 
             * completing a task. Recovering a task is out of the
             * scope of this example and it is application specific.
             *  
             */
            
            zk.create("/tasks", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/workers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/status", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/tasks/task-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/tasks/task-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/tasks/task-003", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/workers/worker-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/assign/worker-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-001/task-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002/task-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/status/task-001", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      
            RecoveredAssignments ra= new RecoveredAssignments(zk);
            ra.recover(new RecoveryCallback(){
                public void recoveryComplete(int rc, List<String> tasks){
                    LOG.info("Completed recovery: " + rc);
                    recoveryDone = true;
                    status = rc;
                    recoveredTasks = tasks;
                }
            });
            
            while(!recoveryDone){
                Thread.sleep(100);
            }
            
            Assert.assertTrue("It hasn't returned ok", status == RecoveryCallback.OK);
            Assert.assertTrue("List size is incorrect: " + recoveredTasks.size(), recoveredTasks.size() == 2);
            Assert.assertTrue("List doesn't contain task-002 ", recoveredTasks.contains("task-002"));
            Assert.assertTrue("List doesn't contain task-003 ", recoveredTasks.contains("task-003"));
            
        } catch (Exception e) {
            LOG.warn("Got exception", e);
            Assert.fail();
        }
        
    }

    @Test(timeout=50000)
    public void testRecoveryStatus(){
        try{
            ZooKeeper zk = new ZooKeeper("localhost:" + port, 10000, new Watcher(){
                public void process(WatchedEvent e) {
                    if(e.getState() == KeeperState.SyncConnected){
                        connected = true;
                    }
                    LOG.info("Event: " + e.toString());
                }
            });
            
            while(!connected){
                Thread.sleep(100);
            }
            
            /*
             * The two tasks that have been assigned to the absent worker
             * have completed, so there is no need to reassign them.
             */
            zk.create("/tasks", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/workers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/status", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/tasks/task-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/tasks/task-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/tasks/task-003", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/workers/worker-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/assign/worker-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-001/task-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002/task-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002/task-003", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            
            zk.create("/status/task-002", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/status/task-003", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            RecoveredAssignments ra= new RecoveredAssignments(zk);
            ra.recover(new RecoveryCallback(){
                public void recoveryComplete(int rc, List<String> tasks){
                    LOG.info("Completed recovery: " + rc);
                    recoveryDone = true;
                    status = rc;
                    recoveredTasks = tasks;
                }
            });
            
            while(!recoveryDone){
                Thread.sleep(100);
            }
            
            Assert.assertTrue("It hasn't returned ok", status == RecoveryCallback.OK);
            Assert.assertTrue("List size is incorrect: " + recoveredTasks.size(), recoveredTasks.size() == 0);
            
        } catch (Exception e) {
            LOG.warn("Got exception", e);
            Assert.fail();
        }   
    }
    
    @Test(timeout=50000)
    public void testRecoveryNoStatus(){
        try{
            ZooKeeper zk = new ZooKeeper("localhost:" + port, 10000, new Watcher(){
                public void process(WatchedEvent e) {
                    if(e.getState() == KeeperState.SyncConnected){
                        connected = true;
                    }
                    LOG.info("Event: " + e.toString());
                }
            });
            
            while(!connected){
                Thread.sleep(100);
            }
            
            /*
             * There is no status znode, so two tasks need to be assigned, one
             * needs to be reassigned because it has been assigned to an absent 
             * worker.
             */
            zk.create("/tasks", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/workers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/status", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/tasks/task-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/tasks/task-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/tasks/task-003", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/workers/worker-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/assign/worker-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-001/task-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002/task-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            RecoveredAssignments ra= new RecoveredAssignments(zk);
            ra.recover(new RecoveryCallback(){
                public void recoveryComplete(int rc, List<String> tasks){
                    LOG.info("Completed recovery: " + rc);
                    recoveryDone = true;
                    status = rc;
                    recoveredTasks = tasks;
                }
            });
            
            while(!recoveryDone){
                Thread.sleep(100);
            }
            
            Assert.assertTrue("It hasn't returned ok", status == RecoveryCallback.OK);
            Assert.assertTrue("List size is incorrect: " + recoveredTasks.size(), recoveredTasks.size() == 2);
            Assert.assertTrue("List doesn't contain task-002 ", recoveredTasks.contains("task-002"));
            Assert.assertTrue("List doesn't contain task-003 ", recoveredTasks.contains("task-003"));
            
        } catch (Exception e) {
            LOG.warn("Got exception", e);
            Assert.fail();
        }
        
    }
    
    @Test(timeout=50000)
    public void testRecoveryMissingTaskFromTasks(){
        try{
            ZooKeeper zk = new ZooKeeper("localhost:" + port, 10000, new Watcher(){
                public void process(WatchedEvent e) {
                    if(e.getState() == KeeperState.SyncConnected){
                        connected = true;
                    }
                    LOG.info("Event: " + e.toString());
                }
            });
            
            while(!connected){
                Thread.sleep(100);
            }
            
            /*
             * Task has been assigned to a worker that has crashed and it is not in the 
             * list of tasks any longer.
             */
            zk.create("/tasks", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/workers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/status", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/tasks/task-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/tasks/task-003", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/workers/worker-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            zk.create("/assign/worker-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-001/task-001", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/assign/worker-002/task-002", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
            RecoveredAssignments ra= new RecoveredAssignments(zk);
            ra.recover(new RecoveryCallback(){
                public void recoveryComplete(int rc, List<String> tasks){
                    LOG.info("Completed recovery: " + rc);
                    recoveryDone = true;
                    status = rc;
                    recoveredTasks = tasks;
                }
            });
            
            while(!recoveryDone){
                Thread.sleep(100);
            }
            
            Assert.assertTrue("It hasn't returned ok", status == RecoveryCallback.OK);
            Assert.assertTrue("List size is incorrect: " + recoveredTasks.size(), recoveredTasks.size() == 2);
            Assert.assertTrue("List doesn't contain task-002 ", recoveredTasks.contains("task-002"));
            Assert.assertTrue("List doesn't contain task-003 ", recoveredTasks.contains("task-003"));
            
        } catch (Exception e) {
            LOG.warn("Got exception", e);
            Assert.fail();
        }
        
    }
    
}
