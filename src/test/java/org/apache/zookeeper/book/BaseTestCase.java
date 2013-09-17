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

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.junit.After;
import org.junit.Before;
import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(BaseTestCase.class);
    
    int port;
    File tmpDir;
    StandaloneServer zkServer;
    final static File BASETEST =
            new File(System.getProperty("buildTestDir", "test"));
    
    @Before
    public void setUp()
    throws IOException {
        this.tmpDir = createTmpDir();
        
        // Start a standalone local zookeeper server.
        this.port = PortAssignment.unique();
        LOG.info("Starting ZooKeeper Standalone Server: " + this.port);
        this.zkServer = new StandaloneServer(this.port, tmpDir);
        this.zkServer.start();
        LOG.info( "ZooKeeper server started" );
    }
    
    @After
    public void tearDown(){
        this.zkServer.shutdown();
        if(tmpDir != null) {
            recursiveDelete(tmpDir);
        }
    }
    
    /**
     * This method stops a zookeeper server and
     * starts a new one. This is to simulate in 
     * tests a zookeeper server going on and off.
     * 
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    void restartServer() 
    throws IOException, InterruptedException {
        this.zkServer.shutdown();
        File confFile = this.zkServer.confFile;
        //Thread.sleep( 1000 );
        this.zkServer = new StandaloneServer(confFile);
        this.zkServer.start();
    }
    
    /**
     * Creates a temporary directory.
     * 
     * @return
     * @throws IOException
     */
    public static File createTmpDir() throws IOException {
        return createTmpDir(BASETEST);
    }
    
    /**
     * Creates a temporary directory under a base directory.
     * 
     * @param parentDir
     * @return
     * @throws IOException
     */
    static File createTmpDir(File parentDir) throws IOException {
        File tmpFile = File.createTempFile("test", ".junit", parentDir);
        // don't delete tmpFile - this ensures we don't attempt to create
        // a tmpDir with a duplicate name
        File tmpDir = new File(tmpFile + ".dir");
        Assert.assertFalse(tmpDir.exists()); // never true if tmpfile does it's job
        Assert.assertTrue(tmpDir.mkdirs());

        return tmpDir;
    }
    
    /**
     * Deletes recursively.
     * 
     * @param d
     * @return
     */
    public static boolean recursiveDelete(File d) {
        if (d.isDirectory()) {
            File children[] = d.listFiles();
            for (File f : children) {
                Assert.assertTrue("delete " + f.toString(), recursiveDelete(f));
            }
        }
        return d.delete();
    }
}
