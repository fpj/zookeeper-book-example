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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    
    ZooKeeper zk;
    String hostPort;
    volatile boolean connected = false;
    volatile boolean expired = false;
    
    Client(String hostPort) { 
        this.hostPort = hostPort;
    }
    
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }
    
    public void process(WatchedEvent e) { 
        System.out.println(e);
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                System.out.println("Exiting due to session expiration");
            default:
                break;
            }
        }
    }
    
    /**
     * Check if this client is connected.
     * 
     * @return
     */
    boolean isConnected(){
        return connected;
    }
    
    /**
     * Check if the ZooKeeper session is expired.
     * 
     * @return
     */
    boolean isExpired(){
        return expired;
    }
    
    /*
     * Executes a sample task and watches for the result
     */
    
    void submitTask(String task, TaskObject taskCtx){
        taskCtx.setTask(task);
        zk.create("/tasks/task-", 
                task.getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback,   
                taskCtx);
    }
    
    StringCallback createTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                /*
                 * Handling connection loss for a sequential node is a bit
                 * delicate. Executing the ZooKeeper create command again
                 * might lead to duplicate tasks. For now, let's assume
                 * that it is ok to create a duplicate task.
                 */
                submitTask(((TaskObject) ctx).getTask(), (TaskObject) ctx);
                
                break;
            case OK:
                LOG.info("My created task name: " + name);
                ((TaskObject) ctx).setTaskName(name);
                watchStatus(name.replace("/tasks/", "/status/"), ctx);
                
                break;
            default:
                LOG.error("Something went wrong" + KeeperException.create(Code.get(rc), path));
            }
        }
    };
    

    protected ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<String, Object>();
    
    void watchStatus(String path, Object ctx){
        ctxMap.put(path, ctx);
        zk.exists(path, 
                statusWatcher, 
                existsCallback, 
                ctx);
    }
    
    Watcher statusWatcher = new Watcher(){
        public void process(WatchedEvent e){
            if(e.getType() == EventType.NodeCreated) {
                assert e.getPath().contains("/status/task-");
                assert ctxMap.containsKey( e.getPath() );
                
                zk.getData(e.getPath(), 
                        false, 
                        getDataCallback, 
                        ctxMap.get(e.getPath()));
            }
        }
    };
    
    StatCallback existsCallback = new StatCallback(){
        public void processResult(int rc, String path, Object ctx, Stat stat){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                watchStatus(path, ctx);
                
                break;
            case OK:
                if(stat != null){
                    zk.getData(path, false, getDataCallback, ctx);
                    LOG.info("Status node is there: " + path);
                } 
                
                break;
            case NONODE:
                break;
            default:     
                LOG.error("Something went wrong when " +
                		"checking if the status node exists: " + 
                        KeeperException.create(Code.get(rc), path));
                
                break;
            }
        }
    };
        
    DataCallback getDataCallback = new DataCallback(){
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                /*
                 * Try again.
                 */
                zk.getData(path, false, getDataCallback, ctxMap.get(path));
                return;
            case OK:
                /*
                 *  Print result
                 */
                String taskResult = new String(data);
                LOG.info("Task " + path + ", " + taskResult);
                
                /*
                 *  Setting the status of the task
                 */
                assert(ctx != null);
                ((TaskObject) ctx).setStatus(taskResult.contains("done"));
                
                /*
                 *  Delete status znode
                 */
                //zk.delete("/tasks/" + path.replace("/status/", ""), -1, taskDeleteCallback, null);
                zk.delete(path, -1, taskDeleteCallback, null);
                ctxMap.remove(path);
                break;
            case NONODE:
                LOG.warn("Status node is gone!");
                return; 
            default:
                LOG.error("Something went wrong here, " + 
                        KeeperException.create(Code.get(rc), path));               
            }
        }
    };
    
    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.delete(path, -1, taskDeleteCallback, null);
                break;
            case OK:
                LOG.info("Successfully deleted " + path);
                break;
            default:
                LOG.error("Something went wrong here, " + 
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };
    
    
    static class TaskObject {
        private String task;
        private String taskName;
        private boolean done = false;
        private boolean succesful = false;
        private CountDownLatch latch = new CountDownLatch(1);
        
        String getTask () {
            return task;
        }
        
        void setTask (String task) {
            this.task = task;
        }
        
        void setTaskName(String name){
            this.taskName = name;
        }
        
        String getTaskName (){
            return taskName;
        }
        
        void setStatus (boolean status){
            succesful = status;
            done = true;
            latch.countDown();
        }
        
        void waitUntilDone () {
            try{
                latch.await();
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException while waiting for task to get done");
            }
        }
        
        synchronized boolean isDone(){
            return done;     
        }
        
        synchronized boolean isSuccesful(){
            return succesful;
        }
        
    }
    
    @Override
    public void close() 
            throws IOException
    {
        LOG.info( "Closing" );
        try{
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
    }
    
    public static void main(String args[]) throws Exception { 
        Client c = new Client(args[0]);
        c.startZK();
        
        while(!c.isConnected()){
            Thread.sleep(100);
        }   
        
        TaskObject task1 = new TaskObject();
        TaskObject task2 = new TaskObject();
        
        c.submitTask("Sample task", task1);
        c.submitTask("Another sample task", task2);
        
        task1.waitUntilDone();
        task2.waitUntilDone();
    }

}
