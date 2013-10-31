package com.objectdriven.maven.zookeeper;


import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.maven.plugin.logging.Log;
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
    static Logger logger;


    /**
     * Configures log4j based off of the Maven Plugin Logger.
     * @param logLevel
     */
    protected void configureLogging( org.apache.maven.plugin.logging.Log logLevel){
        BasicConfigurator.configure();
        logger =  Logger.getLogger("org.apache.zookeeper.server");
        logger.setAdditivity(true);

        if( logLevel == null ) {
            logger.setLevel(Level.INFO);
        } else if( logLevel.isDebugEnabled()){
            logger.setLevel(Level.DEBUG);
        } else if( logLevel.isInfoEnabled()){
            logger.setLevel(Level.INFO);
        } else if( logLevel.isWarnEnabled()){
            logger.setLevel(Level.WARN);
        } else if( logLevel.isErrorEnabled()){
            logger.setLevel(Level.ERROR);
        }
    }

    /**
     * This method populates all of the required parameters for the Zookeeper server, but does no start it.
     *
     * @param port           The network port on which this server will listen
     * @param datadir        the filesystem location to store server data
     * @param tickTime
     * @param maxConnections
     * @throws Exception
     */
    public void configureServer(Integer port, File datadir, Integer tickTime, Integer maxConnections, Log logLevel) throws Exception {
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

        configureLogging(logLevel);

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

    /**
     * Starts the Zookeeper server in a separate thread.
     *
     * @throws Exception
     */
    public void start() throws Exception {
        //Anonymous Inner class to fork running the server process to a different thread.
        Runnable serverRunnable = new Runnable() {
            public void run() {
                try {
                    System.out.println("*********************** ATTEMPT ***************************");
                    cnxnFactory.startup(server);
                    System.out.println("************************");
                    System.out.println("Starting server");
                    System.out.println("************************");
                } catch (IOException e) {
                    throw new RuntimeException("Unable to start", e);
                } catch (InterruptedException e) {
                    System.out.println("********************** INTERUPTED *************************");
                    Thread.interrupted();
                }
            }
        };

        Thread thread = new Thread(serverRunnable, "ZookeeperInProcess-Svr");
        thread.start();
        System.out.println("*********************** Started Server Thread ***************************");
    }


    public void stop() {

        if (server.isRunning()) {
            server.shutdown();
        }

        cnxnFactory.shutdown();
    }
}
