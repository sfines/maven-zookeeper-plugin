package com.objectdriven.maven.zookeeper;


import org.apache.zookeeper.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ZookeeperServerLifecycleTest {

    static Integer port = Integer.valueOf(9999);
    static String hostString = "localhost:" + port;
    static int timeout = 2000;
    static File dataDir = new File(System.getProperty("java.io.tmpdir"));

    static ZookeeperServerLifecycle serverLifecycle;

    /**
     * Configures and invokes an Zookeeper Server inside a separate thread.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void configureLifecycle() throws Exception {
        serverLifecycle = new ZookeeperServerLifecycle();
        serverLifecycle.configureServer(port, dataDir, null, null, null);
        serverLifecycle.start();
    }


    /**
     * This is a basic smoke test- It is to ensure that we can connect
     * to the spawned resource.
     *
     * @throws Exception
     */
    @Test
    public void canConnectToServer() throws Exception {
        ZooKeeper keeper = newZooKeeper();

        keeper.close();
    }

    @Test
    public void canCreateResourcesOnServer() throws Exception {
        ZooKeeper keeper = newZooKeeper();
        try {
            String pth = keeper.create("/test-ip", new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            assertNotNull("Actual path should not have been null...", pth);
        } catch (KeeperException ke) {
            fail(ke.getMessage());
        }

        try {
            keeper.delete("/test-ip", -1);
        } catch (KeeperException ke) {
            fail(ke.getMessage());
        }

    }

    @AfterClass
    public static void terminateServer() throws Exception {
        serverLifecycle.stop();
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
