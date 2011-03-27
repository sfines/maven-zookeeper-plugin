package com.objectdriven.maven.zookeeper;


import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class handles all of the gory details for
 * launching and terminating a Zookeeper Server as part of a maven process
 */
public class ZookeeperServerLifecycle {

    ServerConfig serverConfig = new ServerConfig();
    private NIOServerCnxn.Factory cnxnFactory;
    ZooKeeperServer server;

    public void configureServer(Integer port, File datadir, Integer tickTime, Integer maxConnections) throws Exception {
        List<String> configArguments = new ArrayList<String>();
        if (port == null) {
            throw new IllegalArgumentException("The port must be specified");
        } else {
            configArguments.add(port.toString());
        }

        if (datadir == null) {
            throw new IllegalArgumentException("The datadir must be specified");
        } else {
            if (!datadir.exists()) {
                if (!datadir.mkdirs()) {
                    throw new RuntimeException("Unable to create datadir");
                }
            }
            configArguments.add(datadir.getAbsolutePath());
        }

        if (tickTime != null) {

            configArguments.add(tickTime.toString());
        }

        if (maxConnections != null) {

            if (configArguments.size() == 2) { // No tick time was specified, add a null to pad it
                configArguments.add(null);
            }
            configArguments.add(maxConnections.toString());
        }

        serverConfig.parse(configArguments.toArray(new String[configArguments.size()]));

        server = new ZooKeeperServer();

        FileTxnSnapLog transactionLog = new FileTxnSnapLog(
                new File(serverConfig.getDataLogDir()),
                new File(serverConfig.getDataDir())
        );
        server.setTxnLogFactory(transactionLog);
        server.setTickTime(serverConfig.getTickTime());
        server.setMinSessionTimeout(serverConfig.getMinSessionTimeout());
        server.setMaxSessionTimeout(serverConfig.getMaxSessionTimeout());

        cnxnFactory = new NIOServerCnxn.Factory(
                serverConfig.getClientPortAddress(),
                serverConfig.getMaxClientCnxns()
        );

    }

    public void start() throws Exception {
        //Anonymous Inner class to fork running the server process to a different thread.

        Runnable serverRunnable = new Runnable() {
            public void run() {
                try {
                    cnxnFactory.startup(server);
                } catch (IOException e) {
                    throw new RuntimeException("Unable to start", e);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        };

        Thread thread = new Thread(serverRunnable);
        thread.start();
    }


    public void stop() {

        if (server.isRunning()) {
            server.shutdown();
        }

        cnxnFactory.shutdown();
    }
}
