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

import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Clients are supposed to delete status znodes. If they crash before
 * cleaning up such status znodes, they will hang there forever. This 
 * class cleans up such znodes.
 */

public class OrphanStatuses {
    private static final Logger LOG = LoggerFactory.getLogger(OrphanStatuses.class);
    
    private List<String> tasks;
    private List<String> statuses;
    private ZooKeeper zk;
    
    OrphanStatuses(ZooKeeper zk){
        this.zk = zk;
    }
    
    public void cleanUp(){
        getTasks();
    }
    
    private void getTasks(){
        zk.getChildren("/tasks", false, tasksCallback, null);
    }
    
    ChildrenCallback tasksCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                tasks = children;
                getStatuses();
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
            } 
        }
    };
    
    void getStatuses(){
        zk.getChildren("/status", false, statusCallback, null);
    }

    ChildrenCallback statusCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                statuses = children;
                processTasks();
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
            } 
        }
    };
    
    void processTasks(){
        for(String s: tasks){
            statuses.remove("status-" + s);
        }
        
        for(String s: statuses){
            zk.delete("/status/" + s, -1, deleteStatusCallback, null);
        }
    }
    
    VoidCallback deleteStatusCallback = new VoidCallback(){
      public void processResult(int rc, String path, Object ctx){
          switch (Code.get(rc)) { 
          case CONNECTIONLOSS:
              zk.delete(path, -1, deleteStatusCallback, null);
              break;
          case OK:
              LOG.info("Succesfully deleted orphan status znode: " + path);
              break;
          default:
              LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path)); 
          }
      }
    };
    
    public static void main(String args[]) throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:" + args[0], 10000, new Watcher() {
            public void process(WatchedEvent event) {
                LOG.info( "Received event: " + event.getType() );
            }
        });
        
        (new OrphanStatuses(zk)).cleanUp();
    }
}
