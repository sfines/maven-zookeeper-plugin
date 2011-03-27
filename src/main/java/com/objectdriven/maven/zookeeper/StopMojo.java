package com.objectdriven.maven.zookeeper;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * This goal will issue the shutdown command to the in-process ZooKeeper server.
 *
 * @goal stop
 */
public class StopMojo extends AbstractMojo {

    /**
     * @component
     */
    ZookeeperServerLifecycle zookeeperServerLifecycle;

    public void execute() throws MojoExecutionException, MojoFailureException {
        zookeeperServerLifecycle.stop();
    }
}
