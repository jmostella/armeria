/*
 * Copyright 2016 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.armeria.server.tomcat;

import static java.util.Objects.requireNonNull;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.catalina.Engine;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Service;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerListener;
import com.linecorp.armeria.server.ServerListenerAdapter;
import com.linecorp.armeria.server.ServiceConfig;

/**
 * An {@link HttpService} that dispatches its requests to a web application running in an embedded
 * <a href="https://tomcat.apache.org/">Tomcat</a>.
 *
 * @see TomcatServiceBuilder
 */
public final class TomcatService extends AbstractTomcatService {

    private static final Logger logger = LoggerFactory.getLogger(TomcatService.class);

    private static final Set<String> activeEngines = new HashSet<>();

    /**
     * Creates a new {@link TomcatService} with the web application at the root directory inside the
     * JAR/WAR/directory where the caller class is located at.
     */
    public static TomcatService forCurrentClassPath() {
        return TomcatServiceBuilder.forCurrentClassPath(3).build();
    }

    /**
     * Creates a new {@link TomcatService} with the web application at the specified document base directory
     * inside the JAR/WAR/directory where the caller class is located at.
     */
    public static TomcatService forCurrentClassPath(String docBase) {
        return TomcatServiceBuilder.forCurrentClassPath(docBase, 3).build();
    }

    /**
     * Creates a new {@link TomcatService} with the web application at the root directory inside the
     * JAR/WAR/directory where the specified class is located at.
     */
    public static TomcatService forClassPath(Class<?> clazz) {
        return TomcatServiceBuilder.forClassPath(clazz).build();
    }

    /**
     * Creates a new {@link TomcatService} with the web application at the specified document base directory
     * inside the JAR/WAR/directory where the specified class is located at.
     */
    public static TomcatService forClassPath(Class<?> clazz, String docBase) {
        return TomcatServiceBuilder.forClassPath(clazz, docBase).build();
    }

    /**
     * Creates a new {@link TomcatService} with the web application at the specified document base, which can
     * be a directory or a JAR/WAR file.
     */
    public static TomcatService forFileSystem(String docBase) {
        return TomcatServiceBuilder.forFileSystem(docBase).build();
    }

    /**
     * Creates a new {@link TomcatService} with the web application at the specified document base, which can
     * be a directory or a JAR/WAR file.
     */
    public static TomcatService forFileSystem(Path docBase) {
        return TomcatServiceBuilder.forFileSystem(docBase).build();
    }

    /**
     * Creates a new {@link TomcatService} from an existing {@link Tomcat} instance.
     * If the specified {@link Tomcat} instance is not configured properly, the returned {@link TomcatService}
     * may respond with '503 Service Not Available' error.
     */
    public static TomcatService forTomcat(Tomcat tomcat) {
        requireNonNull(tomcat, "tomcat");

        final String hostname = tomcat.getEngine().getDefaultHost();
        if (hostname == null) {
            throw new IllegalArgumentException("default hostname not configured: " + tomcat);
        }

        final Connector connector = tomcat.getConnector();
        if (connector == null) {
            throw new IllegalArgumentException("connector not configured: " + tomcat);
        }

        return forConnector(hostname, connector);
    }

    /**
     * Creates a new {@link TomcatService} from an existing Tomcat {@link Connector} instance.
     * If the specified {@link Connector} instance is not configured properly, the returned
     * {@link TomcatService} may respond with '503 Service Not Available' error.
     */
    public static TomcatService forConnector(Connector connector) {
        requireNonNull(connector, "connector");
        return new TomcatService(null, hostname -> connector);
    }

    /**
     * Creates a new {@link TomcatService} from an existing Tomcat {@link Connector} instance.
     * If the specified {@link Connector} instance is not configured properly, the returned
     * {@link TomcatService} may respond with '503 Service Not Available' error.
     */
    public static TomcatService forConnector(String hostname, Connector connector) {
        requireNonNull(hostname, "hostname");
        requireNonNull(connector, "connector");

        return new TomcatService(hostname, h -> connector);
    }

    static TomcatService forConfig(TomcatServiceConfig config) {
        final Consumer<Connector> postStopTask = connector -> {
            final org.apache.catalina.Server server = connector.getService().getServer();
            if (server.getState() == LifecycleState.STOPPED) {
                try {
                    logger.info("Destroying an embedded Tomcat: {}", toString(server));
                    server.destroy();
                } catch (Exception e) {
                    logger.warn("Failed to destroy an embedded Tomcat: {}", toString(server), e);
                }
            }
        };

        return new TomcatService(null, new ManagedConnectorFactory(config), postStopTask);
    }

    static String toString(org.apache.catalina.Server server) {
        requireNonNull(server, "server");

        final Service[] services = server.findServices();
        final String serviceName;
        if (services.length == 0) {
            serviceName = "<unknown>";
        } else {
            serviceName = services[0].getName();
        }

        final StringBuilder buf = new StringBuilder(128);

        buf.append("(serviceName: ");
        buf.append(serviceName);
        if (TomcatVersion.major() >= 8) {
            buf.append(", catalinaBase: ");
            buf.append(server.getCatalinaBase());
        }
        buf.append(')');

        return buf.toString();
    }

    private final Function<String, Connector> connectorFactory;
    private final Consumer<Connector> postStopTask;
    private final ServerListener configurator;

    @Nullable
    private org.apache.catalina.Server server;
    @Nullable
    private Server armeriaServer;
    @Nullable
    private String hostname;
    @Nullable
    private Connector connector;
    @Nullable
    private String engineName;
    private boolean started;

    private TomcatService(@Nullable String hostname, Function<String, Connector> connectorFactory) {
        this(hostname, connectorFactory, unused -> { /* unused */ });
    }

    private TomcatService(@Nullable String hostname,
                          Function<String, Connector> connectorFactory, Consumer<Connector> postStopTask) {

        this.hostname = hostname;
        this.connectorFactory = connectorFactory;
        this.postStopTask = postStopTask;
        configurator = new Configurator();
    }

    @Override
    public void serviceAdded(ServiceConfig cfg) throws Exception {
        if (hostname == null) {
            hostname = cfg.server().defaultHostname();
        }

        if (armeriaServer != null) {
            if (armeriaServer != cfg.server()) {
                throw new IllegalStateException("cannot be added to more than one server");
            } else {
                return;
            }
        }

        armeriaServer = cfg.server();
        armeriaServer.addListener(configurator);
    }

    /**
     * Returns Tomcat {@link Connector}.
     */
    @Override
    public Connector connector() {
        final Connector connector = this.connector;
        if (connector == null) {
            throw new IllegalStateException("not started yet");
        }

        return connector;
    }

    @Override
    String hostName() {
        return hostname;
    }

    void start() throws Exception {
        assert hostname != null;
        started = false;
        connector = connectorFactory.apply(hostname);
        final Service service = connector.getService();
        if (service == null) {
            return;
        }

        final Engine engine = TomcatUtil.engine(service, hostname);
        final String engineName = engine.getName();
        if (engineName == null) {
            return;
        }

        if (activeEngines.contains(engineName)) {
            throw new TomcatServiceException("duplicate engine name: " + engineName);
        }

        server = service.getServer();

        if (!TOMCAT_START_STATES.contains(server.getState())) {
            logger.info("Starting an embedded Tomcat: {}", toString(server));
            server.start();
            started = true;
        }

        activeEngines.add(engineName);
        this.engineName = engineName;
    }

    void stop() throws Exception {
        final org.apache.catalina.Server server = this.server;
        final Connector connector = this.connector;
        this.server = null;
        this.connector = null;

        if (engineName != null) {
            activeEngines.remove(engineName);
            engineName = null;
        }

        if (server == null || !started) {
            return;
        }

        try {
            logger.info("Stopping an embedded Tomcat: {}", toString(server));
            server.stop();
        } catch (Exception e) {
            logger.warn("Failed to stop an embedded Tomcat: {}", toString(server), e);
        }

        postStopTask.accept(connector);
    }

    private final class Configurator extends ServerListenerAdapter {
        @Override
        public void serverStarting(Server server) throws Exception {
            start();
        }

        @Override
        public void serverStopped(Server server) throws Exception {
            stop();
        }
    }
}
