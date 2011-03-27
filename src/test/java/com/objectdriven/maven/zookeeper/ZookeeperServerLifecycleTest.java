package com.objectdriven.maven.zookeeper;


import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ZookeeperServerLifecycleTest {

    Integer port = Integer.valueOf(9999);
    String hostString = "localhost:" + port;
    int timeout = 2000;
    File dataDir = new File(System.getProperty("java.io.tmpdir"));

    ZookeeperServerLifecycle serverLifecycle;

    @Before
    public void configureLifecycle() throws Exception {
        serverLifecycle = new ZookeeperServerLifecycle();
        serverLifecycle.configureServer(port, dataDir, null, null);
        serverLifecycle.start();
        Thread.sleep(2000); // give the other thread time to start
    }

    @After
    public void terminateServer() throws Exception {
        serverLifecycle.stop();
        Thread.sleep(5000);
    }


    @Test
    public void canConnectToServer() throws Exception {
        ZooKeeper keeper = newZooKeeper();

        keeper.close();
    }

    @Test
    public void canCreateResourcesOnServer() throws Exception {
        ZooKeeper keeper = newZooKeeper();
        try {
            String pth = keeper.create("/test", new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertNotNull("Actual path should not have been null...", pth);
        } catch (KeeperException ke) {
            fail(ke.getMessage());
        }

    }


    protected ZooKeeper newZooKeeper() throws IOException {
        return new ZooKeeper(hostString, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
    }
}
