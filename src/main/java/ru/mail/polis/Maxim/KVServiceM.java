package ru.mail.polis.Maxim;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import ru.mail.polis.Maxim.Key.KeyInfo;
import ru.mail.polis.Maxim.Key.KeyInfoTask;
import one.nio.net.ConnectionString;
import javafx.util.Pair;

public class KVServiceM extends HttpServer implements KVService {

    @NotNull
    private final KVDaoM kvDao;

    private Map<String, HttpClient> cluster = new HashMap<>();
    private static final ExecutorService threadPool = Executors.newCachedThreadPool();

    public KVServiceM(int port, @NotNull KVDao dao, Set<String> topology) throws IOException {
        super(create(port));
        kvDao = (KVDaoM) dao;
        for (String str : topology) {
            if (Integer.parseInt(str.split(":")[2]) != port) {
                cluster.put(str, new HttpClient(new ConnectionString(str)));
            } else {
                cluster.put(str, null);
            }
        }
    }

    @Path("/v0/status")
    public Response status(Request request) {
        return (request.getMethod() != Request.METHOD_GET)?
                new Response(Response.BAD_REQUEST, Response.EMPTY)
                : Response.ok(Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    public static HttpServerConfig create(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    private Response put(byte[] key, byte[] value, Pair<Integer,Integer> ackFrom, boolean needRepl) {
        List<HttpClient> nodes = getNodes(key, ackFrom);
        if (!needRepl) {
            kvDao.upsert(key, value);
            return new Response(Response.CREATED, Response.EMPTY);
        }

        LinkedList<Future<Integer>> ansList = new LinkedList<>();
        for (HttpClient client : nodes) {
            if (client == null) {
                ansList.add(threadPool.submit(() -> { kvDao.upsert(key, value); return 201; }));
            } else {
                ansList.add(threadPool.submit(() -> { return client.put("/v0/entity?id=" + (new String(key)), value, "X-Need-Repl: 1").getStatus(); }));
            }
        }

        int ack = 0;
        for (Future<Integer> result : ansList) {
            try {
                Integer status = result.get();
                if (status != null && status == 201)
                    ack++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (ack >= ackFrom.getKey()) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response get(byte[] key, Pair<Integer,Integer> ackFrom, boolean needRepl) {
        List<HttpClient> nodes = getNodes(key, ackFrom);
        if (!needRepl) {
            Node node = kvDao.getWithInfo(key);
            if (node.milestone) {
                Response response = new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                response.addHeader("X-Timestamp: " + String.valueOf(node.timestamp));
                return response;
            } else {
                Response response = new Response(Response.OK, node.val);
                response.addHeader("X-Timestamp: " + String.valueOf(node.timestamp));
                return response;
            }
        }

        List<Future<KeyInfo>> ansList = new LinkedList<>();
        for (HttpClient client : nodes) {
            if (client == null) {
                Future<KeyInfo> keyInfoFuture = threadPool.submit(() -> {
                    try {
                        Node node = kvDao.getWithInfo(key);
                        KeyInfo keyInfo = new KeyInfo();
                        if(node.milestone)
                            keyInfo.state = "removed";
                        else
                            keyInfo.state = "exist";
                        keyInfo.timestamp = node.timestamp;
                        keyInfo.value = node.val;
                        return keyInfo;
                    } catch (NoSuchElementException e) {
                        KeyInfo keyInfo = new KeyInfo();
                        keyInfo.state = "none";
                        keyInfo.timestamp = 0;
                        keyInfo.value = null;
                        return keyInfo;
                    }
                });
                ansList.add(keyInfoFuture);
            } else {
                KeyInfoTask task = new KeyInfoTask("/v0/entity?id=" + (new String(key)), client, "X-Need-Repl: 1");
                ansList.add(threadPool.submit(task));
            }
        }

        KeyInfo lastUpdate = null;
        int count = 0;
        for (Future<KeyInfo> keyInfoFuture : ansList) {
            try {
                KeyInfo keyInfo = keyInfoFuture.get();
                count++;
                if (keyInfo.state == "exist") {
                    if (lastUpdate == null || lastUpdate.timestamp > keyInfo.timestamp) {
                        lastUpdate = keyInfo;
                    }
                }
                if (keyInfo.state == "removed") {
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (count < ackFrom.getKey()) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
        if (lastUpdate != null)
            return new Response(Response.OK, lastUpdate.value);
        else
            return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private Response delete(byte[] key, Pair<Integer,Integer> ackFrom, boolean needRepl) {
        List<HttpClient> nodes = getNodes(key, ackFrom);
        if (!needRepl) {
            kvDao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        LinkedList<Future<Integer>> ansList = new LinkedList<>();
        for (HttpClient client : nodes) {
            if (client == null) {
                ansList.add(threadPool.submit(() -> { kvDao.remove(key); return 202; }));
            } else {
                ansList.add(threadPool.submit(() -> { return client.delete("/v0/entity?id=" + (new String(key)), "X-Need-Repl: 1").getStatus(); }));
            }
        }

        int ack = 0;
        for (Future<Integer> result : ansList) {
            try {
                Integer status = result.get();
                if (status != null && status == 202)
                    ack++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (ack >= ackFrom.getKey()) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private List<HttpClient> getNodes(byte[] key, Pair<Integer,Integer> ackFrom) {
        List<HttpClient> nodes = new LinkedList<>();
        String[] hosts = cluster.keySet().toArray(new String[cluster.size()]);
        int currHost = Math.abs(Arrays.hashCode(key) % hosts.length);
        for (int i = 0; i < ackFrom.getValue(); i++) {
            nodes.add(cluster.get(hosts[(currHost + i) % hosts.length]));
        }
        return nodes;
    }

    @Path("/v0/entity")
    public void apiPoint(Request request, HttpSession session) throws IOException {
        String id = request.getParameter("id=");
        String replicas = request.getParameter("replicas=");
        String xNeedRepl = request.getHeader("X-Need-Repl");
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "Bad Request");
            return;
        }
        Pair<Integer,Integer> ackFrom;
        if (replicas != null) {
            String[] split = replicas.split("/");
            ackFrom = new Pair<Integer,Integer>(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
            if (ackFrom.getKey() > ackFrom.getValue() || ackFrom.getKey() == 0) {
                session.sendError(Response.BAD_REQUEST, "Bad Request");
                return;
            }
        } else {
            ackFrom = new Pair<Integer,Integer>(cluster.size() / 2 + 1, cluster.size());
        }
        boolean needRepl = xNeedRepl == null;
        byte[] key = id.getBytes();
        if(request.getMethod() == Request.METHOD_GET) {
                session.sendResponse(get(key, ackFrom, needRepl));
                return;
        }
        else if(request.getMethod() == Request.METHOD_PUT) {
            session.sendResponse(put(key, request.getBody(), ackFrom, needRepl));
            return;
        }
        else if(request.getMethod() == Request.METHOD_DELETE) {
            session.sendResponse(delete(key, ackFrom, needRepl));
            return;
        }
        else{
            session.sendError(Response.BAD_REQUEST, "Unsupported method");
        }
    }
}