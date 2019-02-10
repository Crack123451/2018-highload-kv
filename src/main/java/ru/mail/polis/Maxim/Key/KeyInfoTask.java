package ru.mail.polis.Maxim.Key;

import one.nio.http.HttpClient;
import one.nio.http.Response;

import java.util.concurrent.Callable;


public class KeyInfoTask implements Callable<KeyInfo> {
    private String[] headers;
    private HttpClient client;
    private String path;

    public KeyInfoTask(String path, HttpClient client, String ...headers) {
        this.headers = headers;
        this.client = client;
        this.path = path;
    }

    private static String getHeaderValue(String header) {
        header = header.split(":")[1];
        header = header.trim();
        return header;
    }

    @Override
    public KeyInfo call() throws Exception {
        Response resp = client.get(path, headers);
        KeyInfo keyInfo = new KeyInfo();
        switch(resp.getStatus()) {
            case 504:
                keyInfo.state = "removed";
                keyInfo.timestamp = Long.parseLong(getHeaderValue(resp.getHeader("X-Timestamp")));
                keyInfo.value = resp.getBody();
                return keyInfo;
            case 404:
                keyInfo.state = "none";
                keyInfo.timestamp = Long.parseLong(getHeaderValue(resp.getHeader("X-Timestamp")));
                keyInfo.value = resp.getBody();
                return keyInfo;
            case 200:
                keyInfo.state = "exist";
                keyInfo.timestamp = Long.parseLong(getHeaderValue(resp.getHeader("X-Timestamp")));
                keyInfo.value = resp.getBody();
                return keyInfo;
            default:
                return null;
        }
    }
}
