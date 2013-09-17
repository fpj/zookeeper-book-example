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

import org.apache.zookeeper.book.Master.MasterStates;

import org.junit.Test;
import org.junit.Assert;

public class TestWorkerChanges extends BaseTestCase {
    
    
    @Test(timeout=50000)
    public void addWorker() throws Exception{
        Master m = new Master("localhost:" + port);
        m.startZK();
        
        while(!m.isConnected()){
            Thread.sleep(500);
        }
        
        m.bootstrap();
        m.runForMaster();
        
        while(m.getState() == MasterStates.RUNNING){
            Thread.sleep(100);
        }
        
        Worker w = new Worker("localhost:" + port);
        w.startZK();
        
        while(!w.isConnected()){
            Thread.sleep(100);
        }   
        
        /*
         * bootstrap() create some necessary znodes.
         */
        w.bootstrap();
        
        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        w.register();
        
        while(m.getWorkersSize() == 0){
            Thread.sleep(100);
        }
    }

}
