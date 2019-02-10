package ru.mail.polis.Maxim;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class KVServiceM extends HttpServer implements KVService {

    @NotNull
    private final KVDao kvDao;

    public KVServiceM(int port, @NotNull KVDao dao) throws IOException {
        super(create(port));
        kvDao = dao;
    }

    @Path(value = "/v0/status")
    public Response status(Request request) {
        return (request.getMethod() != Request.METHOD_GET)?
                new Response(Response.BAD_REQUEST, Response.EMPTY)
                : Response.ok(Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        if (!request.getPath().equals("/v0/entity")) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        if(request.getMethod() == Request.METHOD_GET)
            get(session, id);
        else if(request.getMethod() == Request.METHOD_DELETE)
            delete(session, id);
        else if(request.getMethod() == Request.METHOD_PUT)
            put(request, session, id);
        else
            session.sendResponse(new Response(Response.BAD_REQUEST));
    }

    private static HttpServerConfig create(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    private void put(@NotNull Request request, @NotNull HttpSession session, @NotNull String id) throws IOException {
        kvDao.upsert(id.getBytes(Charset.forName("UTF-8")), request.getBody());
        session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
    }

    private void get(@NotNull HttpSession session, @NotNull String id) throws IOException {
        byte[] kvDaoBytes;
        try {
            kvDaoBytes = kvDao.get(id.getBytes(Charset.forName("UTF-8")));
            session.sendResponse(new Response(Response.OK, kvDaoBytes));
        } catch (NoSuchElementException ex) {
            session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
        }
    }

    private void delete(@NotNull HttpSession session, @NotNull String id) throws IOException {
        kvDao.remove(id.getBytes(Charset.forName("UTF-8")));
        session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
    }
}