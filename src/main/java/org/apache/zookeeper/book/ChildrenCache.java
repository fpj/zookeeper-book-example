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

import java.util.ArrayList;
import java.util.List;

/**
 * Auxiliary cache to handle changes to the lists of tasks and of workers.
 */

public class ChildrenCache {

    protected List<String> children;
    
    ChildrenCache() {
        this.children = null;        
    }
    
    ChildrenCache(List<String> children) {
        this.children = children;        
    }
        
    List<String> getList() {
        return children;
    }
        
    List<String> addedAndSet( List<String> newChildren) {
        ArrayList<String> diff = null;
        
        if(children == null) {
            diff = new ArrayList<String>(newChildren);
        } else {
            for(String s: newChildren) {
                if(!children.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
            
        return diff;
    }
        
    List<String> removedAndSet( List<String> newChildren) {
        List<String> diff = null;
            
        if(children != null) {
            for(String s: children) {
                if(!newChildren.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                    
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
        
        return diff;
    }
}
