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

#include "master.h"



/**
 * Watcher we use to process session events. In particular,
 * when it receives a ZOO_CONNECTED_STATE event, we set the
 * connected variable so that we know that the session has
 * been established.
 */
void main_watcher (zhandle_t *zkh,
                   int type,
                   int state,
                   const char *path,
                   void* context)
{
    /*
     * zookeeper_init might not have returned, so we
     * use zkh instead.
     */
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            connected = 1;
            
            LOG_INFO(("Received a connected event."));
        } else if (state == ZOO_NOTCONNECTED_STATE ) {
            connected = 0;
            
            LOG_WARN(("Received a disconnected event."));
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            expired = 1;
            zookeeper_close(zkh);
        }
    }
    LOG_INFO(("Event: ", type2string(type), state));
}

int is_connected() {
    return connected;
}

int is_expired() {
    return expired;
}

/**
 *
 * Assign tasks, but first read task data. In this simple
 * implementation, there is no real task data in the znode,
 * but we include fetching the data to illustrate.
 *
 */
void assign_tasks(const struct String_vector *strings) {
    /*
     * For each task, assign it to a worker.
     */
    LOG_DEBUG(("Task count: %d", strings->count));
    int i;
    for( i = 0; i < strings->count; i++) {
        LOG_DEBUG(("Assigning task %s", (char *) strings->data[i]));
        get_task_data( strings->data[i] );
    }
}


/**
 *
 * Completion function invoked when the call to get
 * the list of tasks returns.
 *
 */
void tasks_completion (int rc,
                       const struct String_vector *strings,
                       const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            get_tasks();
            
            break;
            
        case ZOK:
            LOG_INFO(("Assigning tasks"));
            
            assign_tasks(added_and_set(strings, &tasks));
            
            break;
        default:
            LOG_ERROR(("Something went wrong when checking tasks: %s", rc2string(rc)));
            
            break;
    }
}

/**
 *
 * Watcher function called when the list of tasks
 * changes.
 *
 */
void tasks_watcher (zhandle_t *zh,
                    int type,
                    int state,
                    const char *path,
                    void *watcherCtx) {
    LOG_INFO(("Tasks watcher triggered %s %d", path, state));
    if( type == ZOO_CHILD_EVENT) {
        assert( !strcmp(path, "/tasks") );
        
        get_tasks();
    } else {
        LOG_INFO(("Watched event: ", type2string(type)));
    }
    LOG_INFO(("Tasks watcher done"));
}

static int task_count = 0;

/**
 *
 * Get the list of tasks.
 *
 */
void get_tasks () {
    LOG_INFO(("Getting tasks"));
    zoo_awget_children(zh,
                       "/tasks",
                       tasks_watcher,
                       NULL,
                       tasks_completion,
                       NULL);
}

/**
 *
 * Completion function called when the request
 * to delete the task znode returns.
 *
 */
void delete_task_completion(int rc, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            delete_pending_task((const char*) data);
            
            break;
            
        case ZOK:
            LOG_INFO(("Deleted task: %s", (char*) data));
            free((char *) data);
            break;
            
        default:
            LOG_ERROR(("Something went wrong when deleting task: %s", rc2string(rc)));
            
            break;
    }
}

/**
 *
 * Delete pending task once it has been assigned.
 *
 */
void delete_pending_task (const char* path) {
    char* tmp_path = malloc(sizeof(char) * strlen(path));
    strcpy(tmp_path, path);
    
    zoo_adelete(zh,
                tmp_path,
                -1,
                delete_task_completion,
                (const void*) tmp_path);
}



void workers_completion (int rc,
                         const struct String_vector *strings,
                         const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            get_workers();
            
            break;
            
        case ZOK:
            if(strings->count == 1) {
                LOG_DEBUG(("Got %d worker", strings->count));
            } else {
                LOG_DEBUG(("Got %d workers", strings->count));
            }
            
            if(workers != NULL) {
                free_vector(workers);
            }
            workers = make_copy(strings);
            
            get_tasks();
            
            break;
            
        default:
            LOG_ERROR(("Something went wrong when checking workers: %s", rc2string(rc)));
            
            break;
    }
    
}

void workers_watcher (zhandle_t *zh, int type, int state, const char *path,void *watcherCtx) {
    if( type == ZOO_CHILD_EVENT) {
        assert( !strcmp(path, "/workers") );
        get_workers();
    } else {
        LOG_INFO(("Watched event: ", type2string(type)));
    }
}

void get_workers() {
    zoo_awget_children(zh,
                       "/workers",
                       workers_watcher,
                       NULL,
                       workers_completion,
                       NULL);
}

void take_leadership() {
    get_workers();
}

/*
 * In the case of errors when trying to create the /master lock, we
 * need to check if the znode is there and its content.
 */

void master_check_completion (int rc, const char *value, int value_len,
                              const struct Stat *stat, const void *data) {
    int master_id;
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            check_master();
            
            break;
            
        case ZOK:
            master_id = atoi(value);
            if(master_id == server_id) {
                take_leadership();
                LOG_INFO(("Elected primary master"));
            } else {
                master_exists();
                LOG_INFO(("The primary is some other process"));
            }
            
            break;
            
        case ZNONODE:
            run_for_master();
            
            break;
            
        default:
            LOG_ERROR(("Something went wrong when checking the master lock: %s", rc2string(rc)));
            
            break;
            
    }
}

void check_master () {
    zoo_aget(zh,
             "/master",
             0,
             master_check_completion,
             NULL);
}

void task_assignment_completion (int rc, const char *value, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            task_assignment((struct task_info*) data);
            
            break;
            
        case ZOK:
            /*
             * Delete task from list of pending
             */
            LOG_DEBUG(("Deleting pending task %s", ((struct task_info*) data)->name));
            delete_pending_task(make_path(2, "/tasks/", ((struct task_info*) data)->name));
            free_task_info((struct task_info*) data);
            
            break;
            
        case ZNODEEXISTS:
            LOG_INFO(("Assignment has alreasy been created: %s", value));
            
            break;
            
        default:
            
            LOG_ERROR(("Something went wrong when checking the master lock: %s", rc2string(rc)));
            
            break;
    }
}

void task_assignment(struct task_info *task){
    //Add task to worker list
    char* path = make_path(4, "/assign/" , task->worker, "/", task->name);
    zoo_acreate(zh,
                path,
                task->value,
                task->value_len,
                &ZOO_OPEN_ACL_UNSAFE,
                0,
                task_assignment_completion,
                (const void*) task);
    free(path);
}

void get_task_data_completion(int rc, const char *value, int value_len,
                              const struct Stat *stat, const void *data) {
    int worker_index;
    
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            get_task_data((const char*) data);
            
            break;
            
        case ZOK:
            LOG_INFO(("Choosing worker for task %s", (const char*) data));
            /*
             * Choose worker
             */
            worker_index = (rand() % workers->count);
            LOG_DEBUG(("Chosen worker %d %d", worker_index, workers->count));
            
            /*
             * Assign task to worker
             */
            struct task_info *new_task;
            new_task = (struct task_info*) malloc(sizeof(struct task_info));
            
            new_task->name = (char*) data;
            
            new_task->value = malloc(value_len);
            strcpy(new_task->value, value);
            
            new_task->value_len = value_len;
            
            new_task->worker = malloc(sizeof(char) * strlen(workers->data[worker_index]));
            strcpy(new_task->worker, workers->data[worker_index]);
            
            LOG_DEBUG(("Ready to assign it %d", worker_index));
            
            task_assignment(new_task);
            
            break;
            
        default:
            LOG_ERROR(("Something went wrong when checking the master lock: %s", rc2string(rc)));
            
            break;
            
    }
}

void get_task_data(const char *task) {
    char* path = make_path(2, "/tasks/", task);
    char* tmp_task = malloc(sizeof(char) * strlen(task));
    strcpy(tmp_task, task);
    zoo_aget(zh,
             path,
             0,
             get_task_data_completion,
             (const void *) tmp_task);
    free(path);
    LOG_DEBUG(("Getting task data %s", tmp_task));
}



/*
 * Run for master.
 */


void run_for_master();

void master_exists_watcher (zhandle_t *zh,
                            int type,
                            int state,
                            const char *path,
                            void *watcherCtx) {
    if( type == ZOO_DELETED_EVENT) {
        assert( !strcmp(path, "/master") );
        run_for_master();
    } else {
        LOG_DEBUG(("Watched event: ", type2string(type)));
    }
}

void master_exists_completion (int rc, const struct Stat *stat, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
        case ZOPERATIONTIMEOUT:
            master_exists();
            
            break;
            
        case ZOK:
            if(stat == NULL) {
                LOG_INFO(("Previous master is gone, running for master"));
                run_for_master();
            }
            
            break;
            
        default:
            LOG_WARN(("Something went wrong when executing exists: ", rc2string(rc)));
            
            break;
    }
}

void master_exists() {
    zoo_awexists(zh,
                 "/master",
                 master_exists_watcher,
                 NULL,
                 master_exists_completion,
                 NULL);
}

void master_create_completion (int rc, const char *value, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
            check_master();
            
            break;
            
        case ZOK:
            take_leadership();
            
            break;
            
        case ZNODEEXISTS:
            master_exists();
            
            break;
            
        default:
            LOG_ERROR(("Something went wrong when running for master."));
            
            break;
    }
}

void run_for_master() {
    if(!connected) {
        LOG_WARN(("Client not connected to ZooKeeper"));
        return;
    }
    
    zoo_acreate(zh,
                "/master",
                (const char*) &server_id,
                sizeof(int),
                &ZOO_OPEN_ACL_UNSAFE,
                ZOO_EPHEMERAL,
                master_create_completion,
                NULL);
}

/*
 * Create parent znodes.
 */

void create_parent_completion (int rc, const char *value, const void *data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
            create_parent(value);
            
            break;
            
        case ZOK:
            LOG_INFO(("Created parent node", value));
            
            break;
            
        case ZNODEEXISTS:
            LOG_WARN(("Node already exists"));
            
            break;
            
        default:
            LOG_ERROR(("Something went wrong when running for master"));
            
            break;
    }
}

void create_parent(const char* path) {
    zoo_acreate(zh,
                path,
                "",
                0,
                &ZOO_OPEN_ACL_UNSAFE,
                0,
                create_parent_completion,
                NULL);
    
}

void bootstrap() {
    if(!connected) {
        LOG_WARN(("Client not connected to ZooKeeper"));
        return;
    }
    
    create_parent("/workers");
    create_parent("/assign");
    create_parent("/tasks");
    create_parent("/status");
    
    // Initialize tasks
    tasks = malloc(sizeof(struct String_vector));
    allocate_vector(tasks, 0);
}

int init (char* hostPort) {
    srand(time(NULL));
    server_id  = rand();
    
    zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
    zoo_deterministic_conn_order(1);
    
    zh = zookeeper_init(hostPort,
                        main_watcher,
                        10000,
                        0,
                        0,
                        0);
    
    return errno;
}

void main (int argc, char* argv[]) {
#ifdef THREADED
    
    LOG_INFO(("THREADED defined"));
    if (argc != 2) {
        fprintf(stderr, "USAGE: %s host:port\n", argv[0]);
        exit(1);
    }
    
    /*
     * Initialize ZooKeeper session
     */
    if(init(argv[1])){
        LOG_ERROR(("Error while initializing the master: ", errno));
    }
    
    /*
     * Wait until connected
     */
    while(!is_connected()) {
        sleep(1);
    }
    
    LOG_INFO(("Connected, going to bootstrap and run for master"));
    
    /*
     * Create parent znodes
     */
    bootstrap();
    
    /*
     * Run for master
     */
    run_for_master();
    
    /*
     * Run until session expires
     */
    
    while(!is_expired()) {
        sleep(1);
    }
#else
    int initialized = 0;
    int run = 0;
    fd_set rfds, wfds, efds;

    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    while (!is_expired()) {
        int fd;
        int interest;
        int events;
        struct timeval tv;
        int rc;
        zookeeper_interest(zh, &fd, &interest, &tv);
        if (fd != -1) {
            if (interest&ZOOKEEPER_READ) {
                FD_SET(fd, &rfds);
            } else {
                FD_CLR(fd, &rfds);
            }
            if (interest&ZOOKEEPER_WRITE) {
                FD_SET(fd, &wfds);
            } else {
                FD_CLR(fd, &wfds);
            }
        } else {
            fd = 0;
        }
        //FD_SET(0, &rfds);
        rc = select(fd+1, &rfds, &wfds, &efds, &tv);
        events = 0;
        if (rc > 0) {
            if (FD_ISSET(fd, &rfds)) {
                events |= ZOOKEEPER_READ;
            }
            if (FD_ISSET(fd, &wfds)) {
                events |= ZOOKEEPER_WRITE;
            }
        }
        
        if(!initialized) {
            if(init(argv[1])) {
                LOG_ERROR(("Error while initializing the master: ", errno));
            }
            initialized = 1;
            
        }
        
        if(is_connected() && !run) {
            LOG_INFO(("Connected, going to bootstrap and run for master"));
            
            /*
             * Create parent znodes
             */
            bootstrap();
            
            /*
             * Run for master
             */
            run_for_master();
            
            run =1;
        }
        
        zookeeper_process(zh, events);
    }
#endif
    
}

