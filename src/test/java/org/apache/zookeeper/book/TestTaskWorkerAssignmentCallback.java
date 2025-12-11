package org.apache.zookeeper.book;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

/**
 * A test class for mocking the Master that records the last worker for which
 * getAbsentWorkerTasks was called.
 */
class TestMaster extends Master {
    String lastWorker;

    TestMaster() {
        super("IgnoredForTest");
    }

    @Override
    void getAbsentWorkerTasks(String worker) {
        lastWorker = worker;
    }
}

public class TestTaskWorkerAssignmentCallback {

    @Test
    public void taskWorkerAssignmentCallback() throws Exception {
        TestMaster m = new TestMaster();

        String testWorker = "worker-001";
        m.workerAssignmentCallback.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(),
                "/assign/" + testWorker,
                testWorker,
                null);

        Assert.assertEquals("Last worker not matching", testWorker, m.lastWorker);
    }
}
