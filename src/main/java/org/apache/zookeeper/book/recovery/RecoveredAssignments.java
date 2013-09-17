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

package org.apache.zookeeper.book.recovery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.book.Master;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a task to recover assignments after 
 * a primary master crash. The main idea is to determine the 
 * tasks that have already been assigned and assign the ones
 * that haven't 
 *
 */
public class RecoveredAssignments {
    private static final Logger LOG = LoggerFactory.getLogger(RecoveredAssignments.class);
    
    /*
     * Various lists wew need to keep track of.
     */
    List<String> tasks;
    List<String> assignments;
    List<String> status;
    List<String> activeWorkers;
    List<String> assignedWorkers;
    
    RecoveryCallback cb;
    
    ZooKeeper zk;
    
    /**
     * Callback interface. Called once 
     * recovery completes or fails.
     *
     */
    public interface RecoveryCallback {
        final static int OK = 0;
        final static int FAILED = -1;
        
        public void recoveryComplete(int rc, List<String> tasks);
    }
    
    /**
     * Recover unassigned tasks.
     * 
     * @param zk
     */
    public RecoveredAssignments(ZooKeeper zk){
        this.zk = zk;
        this.assignments = new ArrayList<String>();
    }
    
    /**
     * Starts recovery.
     * 
     * @param recoveryCallback
     */
    public void recover(RecoveryCallback recoveryCallback){
        // Read task list with getChildren
        cb = recoveryCallback;
        getTasks();
    }
    
    private void getTasks(){
        zk.getChildren("/tasks", false, tasksCallback, null);
    }
    
    ChildrenCallback tasksCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                
                break;
            case OK:
                LOG.info("Getting tasks for recovery");
                tasks = children;
                getAssignedWorkers();
                
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
    
    private void getAssignedWorkers(){
        zk.getChildren("/assign", false, assignedWorkersCallback, null);
    }
    
    ChildrenCallback assignedWorkersCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAssignedWorkers();
                
                break;
            case OK:  
                assignedWorkers = children;
                getWorkers(children);

                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
        
    private void getWorkers(Object ctx){
        zk.getChildren("/workers", false, workersCallback, ctx);
    }
    
    
    ChildrenCallback workersCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkers(ctx);
                break;
            case OK:
                LOG.info("Getting worker assignments for recovery: " + children.size());
                
                /*
                 * No worker available yet, so the master is probably let's just return an empty list.
                 */
                if(children.size() == 0) {
                    LOG.warn( "Empty list of workers, possibly just starting" );
                    cb.recoveryComplete(RecoveryCallback.OK, new ArrayList<String>());
                    
                    break;
                }
                
                /*
                 * Need to know which of the assigned workers are active.
                 */
                        
                activeWorkers = children;
                
                for(String s : assignedWorkers){
                    getWorkerAssignments("/assign/" + s);
                }
                
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
    
    private void getWorkerAssignments(String s) {
        zk.getChildren(s, false, workerAssignmentsCallback, null);
    }
    
    ChildrenCallback workerAssignmentsCallback = new ChildrenCallback(){
        public void processResult(int rc, 
                String path, 
                Object ctx, 
                List<String> children) {    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkerAssignments(path);
                break;
            case OK:
                String worker = path.replace("/assign/", "");
                
                /*
                 * If the worker is in the list of active
                 * workers, then we add the tasks to the
                 * assignments list. Otherwise, we need to 
                 * re-assign those tasks, so we add them to
                 * the list of tasks.
                 */
                if(activeWorkers.contains(worker)) {
                    assignments.addAll(children);
                } else {
                    for( String task : children ) {
                        if(!tasks.contains( task )) {
                            tasks.add( task );
                            getDataReassign( path, task );
                        } else {
                            /*
                             * If the task is still in the list
                             * we delete the assignment.
                             */
                            deleteAssignment(path + "/" + task);
                        }
                        
                        /*
                         * Delete the assignment parent. 
                         */
                        deleteAssignment(path);
                    }
                    
                }
                   
                assignedWorkers.remove(worker);
                
                /*
                 * Once we have checked all assignments,
                 * it is time to check the status of tasks
                 */
                if(assignedWorkers.size() == 0){
                    LOG.info("Getting statuses for recovery");
                    getStatuses();
                 }
                
                break;
            case NONODE:
                LOG.info( "No such znode exists: " + path );
                
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
    
    /**
     * Get data of task being reassigned.
     * 
     * @param path
     * @param task
     */
    void getDataReassign(String path, String task) {
        zk.getData(path, 
                false, 
                getDataReassignCallback, 
                task);
    }
    
    /**
     * Context for recreate operation.
     *
     */
    class RecreateTaskCtx {
        String path; 
        String task;
        byte[] data;
        
        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx); 
                
                break;
            case OK:
                recreateTask(new RecreateTaskCtx(path, (String) ctx, data));
                
                break;
            default:
                LOG.error("Something went wrong when getting data ",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Recreate task znode in /tasks
     * 
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
    }
    
    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);
       
                break;
            case OK:
                deleteAssignment(((RecreateTaskCtx) ctx).path);
                
                break;
            case NODEEXISTS:
                LOG.warn("Node shouldn't exist: " + path);
                
                break;
            default:
                LOG.error("Something wwnt wrong when recreating task", 
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * Delete assignment of absent worker
     * 
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path){
        zk.delete(path, -1, taskDeletionCallback, null);
    }
    
    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data" + 
                        KeeperException.create(Code.get(rc), path));
            } 
        }
    };
    
    
    void getStatuses(){
        zk.getChildren("/status", false, statusCallback, null); 
    }
    
    ChildrenCallback statusCallback = new ChildrenCallback(){
        public void processResult(int rc, 
                String path, 
                Object ctx, 
                List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getStatuses();
                
                break;
            case OK:
                LOG.info("Processing assignments for recovery");
                status = children;
                processAssignments();
                
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }
        }
    };
    
    private void processAssignments(){
        LOG.info("Size of tasks: " + tasks.size());
        // Process list of pending assignments
        for(String s: assignments){
            LOG.info("Assignment: " + s);
            deleteAssignment("/tasks/" + s);
            tasks.remove(s);
        }
        
        LOG.info("Size of tasks after assignment filtering: " + tasks.size());
        
        for(String s: status){
            LOG.info( "Checking task: {} ", s );
            deleteAssignment("/tasks/" + s);
            tasks.remove(s);
        }
        LOG.info("Size of tasks after status filtering: " + tasks.size());
        
        // Invoke callback
        cb.recoveryComplete(RecoveryCallback.OK, tasks);     
    }
}
