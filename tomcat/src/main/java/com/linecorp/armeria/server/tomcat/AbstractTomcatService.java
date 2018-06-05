package com.linecorp.armeria.server.tomcat;

import static com.linecorp.armeria.common.util.Functions.voidFunction;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.catalina.LifecycleState;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.util.ServerInfo;
import org.apache.coyote.Adapter;
import org.apache.coyote.InputBuffer;
import org.apache.coyote.OutputBuffer;
import org.apache.coyote.Request;
import org.apache.coyote.Response;
import org.apache.tomcat.util.buf.ByteChunk;
import org.apache.tomcat.util.buf.CharChunk;
import org.apache.tomcat.util.buf.MessageBytes;
import org.apache.tomcat.util.http.MimeHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.linecorp.armeria.common.AggregatedHttpMessage;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpHeaders;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpResponseWriter;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.util.CompletionActions;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.HttpStatusException;
import com.linecorp.armeria.server.ServiceRequestContext;

import io.netty.util.AsciiString;

public abstract class AbstractTomcatService implements HttpService {
    static final Set<LifecycleState> TOMCAT_START_STATES = Sets.immutableEnumSet(
            LifecycleState.STARTED, LifecycleState.STARTING, LifecycleState.STARTING_PREP);
    private static final MethodHandle INPUT_BUFFER_CONSTRUCTOR;
    private static final MethodHandle OUTPUT_BUFFER_CONSTRUCTOR;
    static final Class<?> PROTOCOL_HANDLER_CLASS;

    static {
        final String prefix = TomcatService.class.getPackage().getName() + '.';
        final ClassLoader classLoader = TomcatService.class.getClassLoader();
        final Class<?> inputBufferClass;
        final Class<?> outputBufferClass;
        final Class<?> protocolHandlerClass;
        try {
            if (TomcatVersion.major() < 8 || TomcatVersion.major() == 8 && TomcatVersion.minor() < 5) {
                inputBufferClass = Class.forName(prefix + "Tomcat80InputBuffer", true, classLoader);
                outputBufferClass = Class.forName(prefix + "Tomcat80OutputBuffer", true, classLoader);
                protocolHandlerClass = Class.forName(prefix + "Tomcat80ProtocolHandler", true, classLoader);
            } else {
                inputBufferClass = Class.forName(prefix + "Tomcat90InputBuffer", true, classLoader);
                outputBufferClass = Class.forName(prefix + "Tomcat90OutputBuffer", true, classLoader);
                protocolHandlerClass = Class.forName(prefix + "Tomcat90ProtocolHandler", true, classLoader);
            }

            INPUT_BUFFER_CONSTRUCTOR = MethodHandles.lookup().findConstructor(
                    inputBufferClass, MethodType.methodType(void.class, HttpData.class));
            OUTPUT_BUFFER_CONSTRUCTOR = MethodHandles.lookup().findConstructor(
                    outputBufferClass, MethodType.methodType(void.class, Queue.class));
            PROTOCOL_HANDLER_CLASS = protocolHandlerClass;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "could not find the matching classes for Tomcat version " + ServerInfo.getServerNumber() +
                    "; using a wrong armeria-tomcat JAR?", e);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AbstractTomcatService.class);

    abstract Connector connector();

    abstract String hostName();

    @Override
    public HttpResponse serve(ServiceRequestContext ctx, HttpRequest req) throws Exception {
        final Adapter coyoteAdapter = connector().getProtocolHandler().getAdapter();
        if (coyoteAdapter == null) {
            // Tomcat is not configured / stopped.
            throw HttpStatusException.of(HttpStatus.SERVICE_UNAVAILABLE);
        }

        final HttpResponseWriter res = HttpResponse.streaming();
        req.aggregate().handle(voidFunction((aReq, cause) -> {
            if (cause != null) {
                logger.warn("{} Failed to aggregate a request:", ctx, cause);
                res.close(HttpHeaders.of(HttpStatus.INTERNAL_SERVER_ERROR));
                return;
            }

            try {
                final Request coyoteReq = convertRequest(ctx, aReq);
                if (coyoteReq == null) {
                    res.close(HttpHeaders.of(HttpStatus.BAD_REQUEST));
                    return;
                }
                final Response coyoteRes = new Response();
                coyoteReq.setResponse(coyoteRes);
                coyoteRes.setRequest(coyoteReq);

                final Queue<HttpData> data = new ArrayDeque<>();
                coyoteRes.setOutputBuffer((OutputBuffer) OUTPUT_BUFFER_CONSTRUCTOR.invoke(data));

                ctx.blockingTaskExecutor().execute(() -> {
                    if (!res.isOpen()) {
                        return;
                    }

                    try {
                        coyoteAdapter.service(coyoteReq, coyoteRes);
                        final HttpHeaders headers = convertResponse(coyoteRes);
                        if (res.tryWrite(headers)) {
                            for (; ; ) {
                                final HttpData d = data.poll();
                                if (d == null || !res.tryWrite(d)) {
                                    break;
                                }
                            }
                        }
                    } catch (Throwable t) {
                        logger.warn("{} Failed to produce a response:", ctx, t);
                    } finally {
                        res.close();
                    }
                });
            } catch (Throwable t) {
                logger.warn("{} Failed to invoke Tomcat:", ctx, t);
                res.close();
            }
        })).exceptionally(CompletionActions::log);

        return res;
    }

    @Nullable
    private Request convertRequest(ServiceRequestContext ctx, AggregatedHttpMessage req) throws Throwable {
        final String mappedPath = ctx.mappedPath();
        final Request coyoteReq = new Request();

        coyoteReq.scheme().setString(req.scheme());

        // Set the remote host/address.
        final InetSocketAddress remoteAddr = ctx.remoteAddress();
        coyoteReq.remoteAddr().setString(remoteAddr.getAddress().getHostAddress());
        coyoteReq.remoteHost().setString(remoteAddr.getHostString());
        coyoteReq.setRemotePort(remoteAddr.getPort());

        // Set the local host/address.
        final InetSocketAddress localAddr = ctx.localAddress();
        coyoteReq.localAddr().setString(localAddr.getAddress().getHostAddress());
        coyoteReq.localName().setString(hostName());
        coyoteReq.setLocalPort(localAddr.getPort());

        final String hostHeader = req.headers().authority();
        final int colonPos = hostHeader.indexOf(':');
        if (colonPos < 0) {
            coyoteReq.serverName().setString(hostHeader);
        } else {
            coyoteReq.serverName().setString(hostHeader.substring(0, colonPos));
            try {
                final int port = Integer.parseInt(hostHeader.substring(colonPos + 1));
                coyoteReq.setServerPort(port);
            } catch (NumberFormatException e) {
                // Invalid port number
                return null;
            }
        }

        // Set the method.
        final HttpMethod method = req.method();
        coyoteReq.method().setString(method.name());

        // Set the request URI.
        final byte[] uriBytes = mappedPath.getBytes(StandardCharsets.US_ASCII);
        coyoteReq.requestURI().setBytes(uriBytes, 0, uriBytes.length);

        // Set the query string if any.
        if (ctx.query() != null) {
            coyoteReq.queryString().setString(ctx.query());
        }

        // Set the headers.
        final MimeHeaders cHeaders = coyoteReq.getMimeHeaders();
        convertHeaders(req.headers(), cHeaders);
        convertHeaders(req.trailingHeaders(), cHeaders);

        // Set the content.
        final HttpData content = req.content();
        coyoteReq.setInputBuffer((InputBuffer) INPUT_BUFFER_CONSTRUCTOR.invoke(content));

        return coyoteReq;
    }

    private static void convertHeaders(HttpHeaders headers, MimeHeaders cHeaders) {
        if (headers.isEmpty()) {
            return;
        }

        for (Entry<AsciiString, String> e : headers) {
            final AsciiString k = e.getKey();
            final String v = e.getValue();

            if (k.isEmpty() || k.byteAt(0) == ':') {
                continue;
            }

            final MessageBytes cValue = cHeaders.addValue(k.array(), k.arrayOffset(), k.length());
            final byte[] valueBytes = v.getBytes(StandardCharsets.US_ASCII);
            cValue.setBytes(valueBytes, 0, valueBytes.length);
        }
    }

    private static HttpHeaders convertResponse(Response coyoteRes) {
        final HttpHeaders headers = HttpHeaders.of(HttpStatus.valueOf(coyoteRes.getStatus()));

        final String contentType = coyoteRes.getContentType();
        if (contentType != null && !contentType.isEmpty()) {
            headers.set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }

        final long contentLength = coyoteRes.getBytesWritten(true); // 'true' will trigger flush.
        final String method = coyoteRes.getRequest().method().toString();
        if (!"HEAD".equals(method)) {
            headers.setLong(HttpHeaderNames.CONTENT_LENGTH, contentLength);
        }

        final MimeHeaders cHeaders = coyoteRes.getMimeHeaders();
        final int numHeaders = cHeaders.size();
        for (int i = 0; i < numHeaders; i++) {
            final AsciiString name = toHeaderName(cHeaders.getName(i));
            if (name == null) {
                continue;
            }

            final String value = toHeaderValue(cHeaders.getValue(i));
            if (value == null) {
                continue;
            }

            headers.add(name.toLowerCase(), value);
        }

        return headers;
    }

    @Nullable
    private static AsciiString toHeaderName(MessageBytes value) {
        switch (value.getType()) {
            case MessageBytes.T_BYTES: {
                final ByteChunk chunk = value.getByteChunk();
                return new AsciiString(chunk.getBuffer(), chunk.getOffset(), chunk.getLength(), true);
            }
            case MessageBytes.T_CHARS: {
                final CharChunk chunk = value.getCharChunk();
                return new AsciiString(chunk.getBuffer(), chunk.getOffset(), chunk.getLength());
            }
            case MessageBytes.T_STR: {
                return HttpHeaderNames.of(value.getString());
            }
        }
        return null;
    }

    @Nullable
    private static String toHeaderValue(MessageBytes value) {
        switch (value.getType()) {
            case MessageBytes.T_BYTES: {
                final ByteChunk chunk = value.getByteChunk();
                return new String(chunk.getBuffer(), chunk.getOffset(), chunk.getLength(),
                                  StandardCharsets.US_ASCII);
            }
            case MessageBytes.T_CHARS: {
                final CharChunk chunk = value.getCharChunk();
                return new String(chunk.getBuffer(), chunk.getOffset(), chunk.getLength());
            }
            case MessageBytes.T_STR: {
                return value.getString();
            }
        }
        return null;
    }
}
