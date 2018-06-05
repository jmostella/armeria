package com.linecorp.armeria.server.tomcat;

import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;

import com.linecorp.armeria.server.HttpService;

/**
 * An {@link HttpService} that dispatches its requests to a web application running in an embedded
 * <a href="https://tomcat.apache.org/">Tomcat</a>.
 *
 * This Implementation does not manage the container. Useful for implementations such as Spring Boot.
 */
public class UnmanagedTomcatService extends AbstractTomcatService {

    private Tomcat tomcat;

    public UnmanagedTomcatService(Tomcat tomcat) {
        this.tomcat = tomcat;
    }

    @Override
    Connector connector() {
        return tomcat.getConnector();
    }

    @Override
    String hostName() {
        return tomcat.getEngine().getDefaultHost();
    }
}
