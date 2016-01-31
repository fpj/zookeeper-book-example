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




/*
 * Auxiliary functions
 */

char * make_path(int num, ...) {
    const char * tmp_string;
    
    va_list arguments;
    va_start ( arguments, num );
    
    int total_length = 0;
    int x;
    for ( x = 0; x < num; x++ ) {
        tmp_string = va_arg ( arguments, const char * );
        if(tmp_string != NULL) {
            LOG_DEBUG(("Counting path with this path %s (%d)", tmp_string, num));
            total_length += strlen(tmp_string);
        }
    }

    va_end ( arguments );

    char * path = malloc(total_length * sizeof(char) + 1);
    path[0] = '\0';
    va_start ( arguments, num );
    
    for ( x = 0; x < num; x++ ) {
        tmp_string = va_arg ( arguments, const char * );
        if(tmp_string != NULL) {
            LOG_DEBUG(("Counting path with this path %s",
                       tmp_string));
            strcat(path, tmp_string);
        }
    }

    return path;
}

struct String_vector* make_copy( const struct String_vector* vector ) {
    struct String_vector* tmp_vector = malloc(sizeof(struct String_vector));
    
    tmp_vector->data = malloc(vector->count * sizeof(const char *));
    tmp_vector->count = vector->count;
    
    int i;
    for( i = 0; i < vector->count; i++) {
        tmp_vector->data[i] = strdup(vector->data[i]);
    }
    
    return tmp_vector;
}


/*
 * Allocate String_vector, copied from zookeeper.jute.c
 */
int allocate_vector(struct String_vector *v, int32_t len) {
    if (!len) {
        v->count = 0;
        v->data = 0;
    } else {
        v->count = len;
        v->data = calloc(sizeof(*v->data), len);
    }
    return 0;
}


/*
 * Functions to free memory
 */
void free_vector(struct String_vector* vector) {
    int i;
    
    // Free each string
    for(i = 0; i < vector->count; i++) {
        free(vector->data[i]);
    }
    
    // Free data
    free(vector -> data);
    
    // Free the struct
    free(vector);
}

void free_task_info(struct task_info* task) {
    free(task->name);
    free(task->value);
    free(task->worker);
    free(task);
}

/*
 * Functions to deal with workers and tasks caches
 */

int contains(const char * child, const struct String_vector* children) {
  int i;
  for(i = 0; i < children->count; i++) {
    if(!strcmp(child, children->data[i])) {
      return 1;
    }
  }

  return 0;
}

/*
 * This function returns the elements that are new in current
 * compared to previous and update previous.
 */
struct String_vector* added_and_set(const struct String_vector* current,
                                    struct String_vector** previous) {
    struct String_vector* diff = malloc(sizeof(struct String_vector));
    
    int count = 0;
    int i;
    for(i = 0; i < current->count; i++) {
        if (!contains(current->data[i], (*previous))) {
            count++;
        }
    }
    
    allocate_vector(diff, count);
    
    int prev_count = count;
    count = 0;
    for(i = 0; i < current->count; i++) {
        if (!contains(current->data[i], (* previous))) {
            diff->data[count] = malloc(sizeof(char) * strlen(current->data[i]) + 1);
            memcpy(diff->data[count++],
                   current->data[i],
                   strlen(current->data[i]));
        }
    }
    
    assert(prev_count == count);
    
    free_vector((struct String_vector*) *previous);
    (*previous) = make_copy(current);
    
    return diff;
    
}

/*
 * This function returns the elements that are have been removed in
 * compared to previous and update previous.
 */
struct String_vector* removed_and_set(const struct String_vector* current,
                                      struct String_vector** previous) {
    
    struct String_vector* diff = malloc(sizeof(struct String_vector));
    
    int count = 0;
    int i;
    for(i = 0; i < (* previous)->count; i++) {
        if (!contains((* previous)->data[i], current)) {
            count++;
        }
    }
    
    allocate_vector(diff, count);
    
    int prev_count = count;
    count = 0;
    for(i = 0; i < (* previous)->count; i++) {
        if (!contains((* previous)->data[i], current)) {
            diff->data[count] = malloc(sizeof(char) * strlen((* previous)->data[i]));
            strcpy(diff->data[count++], (* previous)->data[i]);
        }
    }

    assert(prev_count == count);

    free_vector((struct String_vector*) *previous);
    (*previous) = make_copy(current);
    
    return diff;
}

/*
 * End of auxiliary functions, it is all related to zookeeper
 * from this point on.
 */

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

            LOG_DEBUG(("Received a connected event."));
        } else if (state == ZOO_CONNECTING_STATE) {
            if(connected == 1) {
                LOG_WARN(("Disconnected."));
            }
            connected = 0;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            expired = 1;
            connected = 0;
            zookeeper_close(zkh);
        }
    }
    LOG_DEBUG(("Event: %s, %d", type2string(type), state));
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
        LOG_DEBUG(("Assigning task %s",
                   (char *) strings->data[i]));
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
            LOG_DEBUG(("Assigning tasks"));

            struct String_vector *tmp_tasks = added_and_set(strings, &tasks);
            assign_tasks(tmp_tasks);
            free_vector(tmp_tasks);

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
    LOG_DEBUG(("Tasks watcher triggered %s %d", path, state));
    if( type == ZOO_CHILD_EVENT) {
        assert( !strcmp(path, "/tasks") );
        get_tasks();
    } else {
        LOG_INFO(("Watched event: %s", type2string(type)));
    }
    LOG_DEBUG(("Tasks watcher done"));
}

static int task_count = 0;

/**
 *
 * Get the list of tasks.
 *
 */
void get_tasks () {
  LOG_DEBUG(("Getting tasks"));
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
            delete_pending_task((const char *) data);
            
            break;
        case ZOK:
            LOG_DEBUG(("Deleted task: %s", (char *) data));
            free((char *) data);
            
            break;
        default:
            LOG_ERROR(("Something went wrong when deleting task: %s",
                       rc2string(rc)));

            break;
    }
}

/**
 *
 * Delete pending task once it has been assigned.
 *
 */
void delete_pending_task (const char * path) {
    if(path == NULL) return;
    
    char * tmp_path = strdup(path);
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

            struct String_vector *tmp_workers = removed_and_set(strings, &workers);
            free_vector(tmp_workers);
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
        LOG_DEBUG(("Watched event: ", type2string(type)));
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
            sscanf(value, "%x", &master_id );
            if(master_id == server_id) {
                take_leadership();
                LOG_DEBUG(("Elected primary master"));
            } else {
                master_exists();
                LOG_DEBUG(("The primary is some other process"));
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
            if(data != NULL) {
                /*
                 * Delete task from list of pending
                 */
                LOG_DEBUG(("Deleting pending task %s",
                           ((struct task_info*) data)->name));
                char * del_path = "";
                del_path = make_path(2, "/tasks/", ((struct task_info*) data)->name);
                if(del_path != NULL) {
                    delete_pending_task(del_path);
                }
                free(del_path);
                free_task_info((struct task_info*) data);
            }

            break;
        case ZNODEEXISTS:
            LOG_DEBUG(("Assignment has already been created: %s", value));

            break;
        default:
            LOG_ERROR(("Something went wrong when checking assignment completion: %s", rc2string(rc)));

            break;
    }
}

void task_assignment(struct task_info *task){
    //Add task to worker list
    char * path = make_path(4, "/assign/" , task->worker, "/", task->name);
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
            get_task_data((const char *) data);
            
            break;
            
        case ZOK:
            LOG_DEBUG(("Choosing worker for task %s", (const char *) data));
            if(workers != NULL) {
                /*
                 * Choose worker
                 */
                worker_index = (rand() % workers->count);
                LOG_DEBUG(("Chosen worker %d %d",
                           worker_index, workers->count));
            
                /*
                 * Assign task to worker
                 */
                struct task_info *new_task;
                new_task = (struct task_info*) malloc(sizeof(struct task_info));
            
                new_task->name = (char *) data;
                new_task->value = strndup(value, value_len);
                new_task->value_len = value_len;

                const char * worker_string = workers->data[worker_index];
                new_task->worker = strdup(worker_string);
            
                LOG_DEBUG(("Ready to assign it %d, %s",
                           worker_index,
                           workers->data[worker_index]));
                task_assignment(new_task);
            }

            break;
        default:
            LOG_ERROR(("Something went wrong when checking the master lock: %s",
                       rc2string(rc)));
            
            break;
    }
}

void get_task_data(const char *task) {
    if(task == NULL) return;
    
    LOG_DEBUG(("Task path: %s",
               task));
    char * tmp_task = strndup(task, 15);
    char * path = make_path(2, "/tasks/", tmp_task);
    LOG_DEBUG(("Getting task data %s",
               tmp_task));
    
    zoo_aget(zh,
             path,
             0,
             get_task_data_completion,
             (const void *) tmp_task);
    free(path);
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
            break;
        case ZNONODE:
            LOG_INFO(("Previous master is gone, running for master"));
            run_for_master();

            break;
        default:
            LOG_WARN(("Something went wrong when executing exists: %s", rc2string(rc)));

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
    
    char server_id_string[9];
    snprintf(server_id_string, 9, "%x", server_id);
    zoo_acreate(zh,
                "/master",
                (const char *) server_id_string,
                strlen(server_id_string) + 1,
                &ZOO_OPEN_ACL_UNSAFE,
                ZOO_EPHEMERAL,
                master_create_completion,
                NULL);
}

/*
 * Create parent znodes.
 */

void create_parent_completion (int rc, const char * value, const void * data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
            create_parent(value, (const char *) data);
            
            break;
        case ZOK:
            LOG_INFO(("Created parent node", value));

            break;
        case ZNODEEXISTS:
            LOG_WARN(("Node already exists"));
            
            break;
        default:
	  LOG_ERROR(("Something went wrong when running for master: %s, %s", value, rc2string(rc)));
            
            break;
    }
}

void create_parent(const char * path, const char * value) {
    zoo_acreate(zh,
                path,
                value,
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
    
    create_parent("/workers", "");
    create_parent("/assign", "");
    create_parent("/tasks", "");
    create_parent("/status", "");
    
    // Initialize tasks
    tasks = malloc(sizeof(struct String_vector));
    allocate_vector(tasks, 0);
    workers = malloc(sizeof(struct String_vector));
    allocate_vector(workers, 0);
}

int init (char * hostPort) {
    srand(time(NULL));
    server_id  = rand();
    
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    
    zh = zookeeper_init(hostPort,
                        main_watcher,
                        15000,
                        0,
                        0,
                        0);
    
    return errno;
}

int main (int argc, char * argv[]) {
    LOG_DEBUG(("THREADED defined"));
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
    
#ifdef THREADED
    /*
     * Wait until connected
     */
    while(!is_connected()) {
        sleep(1);
    }
    
    LOG_DEBUG(("Connected, going to bootstrap and run for master"));
    
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
    int run = 0;
    fd_set rfds, wfds, efds;

    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    while (!is_expired()) {
        int fd = -1;
        int interest = 0;
        int events ;
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
        
        /*
         * The next if block contains 
         * calls to bootstrap the master
         * and run for master. We only
         * get into it when the client
         * has established a session and
         * is_connected is true.
         */
        if(is_connected() && !run) {
            LOG_DEBUG(("Connected, going to bootstrap and run for master"));
            
            /*
             * Create parent znodes
             */
            bootstrap();
            
            /*
             * Run for master
             */
            run_for_master();
            
            run = 1;
        }
        
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
        
        zookeeper_process(zh, events);
    }
#endif
    
    return 0; 
}

