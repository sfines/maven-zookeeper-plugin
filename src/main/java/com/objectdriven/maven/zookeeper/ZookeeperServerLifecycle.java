package com.objectdriven.maven.zookeeper;


import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.codehaus.plexus.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  This class handles all of the gory details for
 *  launching and terminating a Zookeeper Server as part of a maven process
 */
public class ZookeeperServerLifecycle {

    ServerConfig serverConfig = new ServerConfig();
    private NIOServerCnxn.Factory cnxnFactory;
    ZooKeeperServer server;

    public void configureServer(String port, String datadir, String tickTime, String maxConnections) throws Exception{
          List<String> configArguments = new ArrayList<String>();
        if( StringUtils.isEmpty(port) ) {
            throw new IllegalArgumentException("The port must be specified");
        } else {
            configArguments.add(port);
        }

        if( StringUtils.isEmpty(datadir)){
            throw new IllegalArgumentException("The datadir must be specified");
        } else {
            configArguments.add(datadir);
        }

        if( StringUtils.isNotEmpty(tickTime)){
            if( !StringUtils.isNumeric(tickTime)){
                throw new IllegalArgumentException("The ticktime argument was set, but it was not numeric");
            }
            configArguments.add(tickTime);
        }

        if( StringUtils.isNotEmpty(maxConnections)){
            if(!StringUtils.isNumeric(maxConnections)){
                throw new IllegalArgumentException("The maxConnections argument was set, but it was not numeric");
            }
            if( configArguments.size() == 2){ // No tick time was specified, add a null to pad it
                configArguments.add(null);
            }
            configArguments.add( maxConnections);
        }

        serverConfig.parse(configArguments.toArray(new String[4]));

        server = new ZooKeeperServer();

        FileTxnSnapLog transactionLog = new FileTxnSnapLog(
                new File( serverConfig.getDataLogDir()),
                new File( serverConfig.getDataDir())
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

    public void start() throws Exception{
        //Anonymous Inner class to fork running the server process to a different thread.

        Runnable serverRunnable = new Runnable(){
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

        if( server.isRunning()){
            server.shutdown();
        }

        cnxnFactory.shutdown();
    }
}
