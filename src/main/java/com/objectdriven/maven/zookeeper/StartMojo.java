package com.objectdriven.maven.zookeeper;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * @goal start
 */
public class StartMojo extends AbstractMojo{


    /**
     * @component
     */
    ZookeeperServerLifecycle zookeeperServerLifecycle;

    public void execute() throws MojoExecutionException, MojoFailureException {

    }
}
