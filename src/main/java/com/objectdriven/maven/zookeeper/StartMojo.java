package com.objectdriven.maven.zookeeper;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import java.io.File;

/**
 * @goal start
 */
public class StartMojo extends AbstractMojo {

    /**
     * Where will zookeeper store its data
     *
     * @parameter optional="false" expression="${zoo.dataDirectory}" default-value="${project.build.directory}/zookeeperData/"
     */
    private File dataDirectory;

    /**
     * On which port will zookeeper listen
     *
     * @parameter optional="false" expression="${zoo.port}" default-value=3000
     */
    private Integer port;

    /**
     * Represents clock tick time (optional)
     *
     * @parameter expression="${zoo.tickTime}" optional="true"
     */
    private Integer tickTime;

    /**
     * Represents the maximum number of connections that this zookeeper process will
     * service
     *
     * @parameter expression="${zoo.maxConnections}" optional="true"
     */
    private Integer maxConnections;

    /**
     * @component
     */
    private ZookeeperServerLifecycle zookeeperServerLifecycle;



    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            zookeeperServerLifecycle.configureServer(port, dataDirectory, tickTime, maxConnections, super.getLog());

            zookeeperServerLifecycle.start();
        } catch (Exception e) {
            throw new MojoExecutionException("Unable to start the Zookeeper Server", e);
        }
    }
}
