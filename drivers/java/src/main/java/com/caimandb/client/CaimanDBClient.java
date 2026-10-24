package com.caimandb.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * CaimanDB driver for Java.
 *
 * <p>Talks to CaimanDB's query HTTP server (default port 1555) and,
 * optionally, its admin HTTP server (default port 1556) for login.
 * Uses only {@code java.net.http.HttpClient} (Java 11+) -- no external
 * dependencies. JSON encoding/decoding is hand-rolled (see {@link Json})
 * to avoid pulling in a JSON library; it covers the shapes CaimanDB's
 * API actually returns (objects, arrays, strings, numbers, booleans,
 * null).
 *
 * <p>Wire protocol (see docs/api/http-api.md in the CaimanDB repo):
 * <pre>
 *   POST {queryUrl}/query?query=&lt;NQL&gt;&amp;db=&lt;db&gt;   -&gt; {success, result, db}
 *   GET  {queryUrl}/health
 *   GET  {queryUrl}/status
 *   GET  {queryUrl}/watch?db=&amp;block=              -&gt; Server-Sent Events
 *   POST {adminUrl}/api/v1/auth/login  {username, password}
 *                                                     -&gt; {token, user, role, expires}
 * </pre>
 *
 * <p>Auth: either HTTP Basic (username/password) or a JWT Bearer token
 * obtained via {@link #login} or supplied directly.
 */
public class CaimanDBClient {

    private final String queryUrl;
    private final String adminUrl;
    private String username;
    private String password;
    private String token;
    private final String defaultDb;
    private final HttpClient http;

    public static class CaimanDBException extends RuntimeException {
        public final int status;
        public final Map<String, Object> body;

        public CaimanDBException(String message, int status, Map<String, Object> body) {
            super(message);
            this.status = status;
            this.body = body;
        }
    }

    /** Configuration for a new {@link CaimanDBClient}. */
    public static class Options {
        public String queryUrl = "http://localhost:1555";
        public String adminUrl = "http://localhost:1556";
        public String username;
        public String password;
        public String token;
        public String db = "default";
        public Duration timeout = Duration.ofSeconds(30);
    }

    public CaimanDBClient(Options opts) {
        this.queryUrl = stripTrailingSlash(opts.queryUrl);
        this.adminUrl = stripTrailingSlash(opts.adminUrl);
        this.username = opts.username;
        this.password = opts.password;
        this.token = opts.token;
        this.defaultDb = opts.db;
        this.http = HttpClient.newBuilder().connectTimeout(opts.timeout).build();
    }

    public CaimanDBClient() {
        this(new Options());
    }

    private static String stripTrailingSlash(String url) {
        if (url != null && url.endsWith("/")) {
            return url.substring(0, url.length() - 1);
        }
        return url;
    }

    private HttpRequest.Builder authorized(HttpRequest.Builder builder) {
        if (token != null && !token.isEmpty()) {
            builder.header("Authorization", "Bearer " + token);
        } else if (username != null) {
            String raw = username + ":" + (password == null ? "" : password);
            String encoded = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
            builder.header("Authorization", "Basic " + encoded);
        }
        return builder;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> request(String method, String url, Map<String, Object> jsonBody) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url));
        if (jsonBody != null) {
            String encoded = Json.encode(jsonBody);
            builder.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(encoded, StandardCharsets.UTF_8));
        } else {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        }
        authorized(builder);

        HttpResponse<String> response;
        try {
            response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new CaimanDBException("request failed: " + e.getMessage(), 0, Map.of());
        }

        Map<String, Object> parsed;
        String raw = response.body();
        if (raw != null && !raw.isEmpty()) {
            Object decoded = Json.decode(raw);
            parsed = (decoded instanceof Map) ? (Map<String, Object>) decoded : Map.of("raw", raw);
        } else {
            parsed = Map.of();
        }

        if (response.statusCode() >= 400) {
            Object err = parsed.getOrDefault("error", parsed.get("result"));
            String message = err != null ? String.valueOf(err) : ("request failed (" + response.statusCode() + ")");
            throw new CaimanDBException(message, response.statusCode(), parsed);
        }
        return parsed;
    }

    /**
     * Logs in against the admin server and stores the JWT for
     * subsequent requests.
     *
     * @return the parsed response: {@code {token, user, role, expires}}
     */
    public Map<String, Object> login(String username, String password) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("username", username);
        body.put("password", password);
        Map<String, Object> resp = request("POST", adminUrl + "/api/v1/auth/login", body);
        Object t = resp.get("token");
        if (t != null) {
            this.token = String.valueOf(t);
        }
        this.username = username;
        return resp;
    }

    /**
     * Executes a raw NQL command, e.g. {@code FIND users WHERE age > 18}.
     */
    public Map<String, Object> query(String nql, String db) {
        String effectiveDb = (db == null || db.isEmpty()) ? defaultDb : db;
        String encodedQuery = urlEncode(nql);
        String encodedDb = urlEncode(effectiveDb);
        String url = queryUrl + "/query?query=" + encodedQuery + "&db=" + encodedDb;
        Map<String, Object> body = request("POST", url, null);
        Object success = body.get("success");
        if (success instanceof Boolean && !((Boolean) success)) {
            throw new CaimanDBException(String.valueOf(body.get("result")), 0, body);
        }
        return body;
    }

    public Map<String, Object> query(String nql) {
        return query(nql, null);
    }

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    // ---- Convenience wrappers over query() -------------------------------

    /** INSERT one document (a Map) or several (a List of Maps). */
    public Map<String, Object> insert(String block, Object docOrDocs, String db) {
        String payload = Json.encode(docOrDocs);
        return query("INSERT " + block + " " + payload, db);
    }

    public Map<String, Object> insert(String block, Object docOrDocs) {
        return insert(block, docOrDocs, null);
    }

    /** GET a single document by id. */
    public Map<String, Object> get(String block, String id, String db) {
        return query("GET " + block + " " + id, db);
    }

    public Map<String, Object> get(String block, String id) {
        return get(block, id, null);
    }

    /** Options for {@link #find}. */
    public static class FindOptions {
        public String where;
        public List<String> select;
        public String order;
        public Integer limit;
        public Integer offset;
    }

    /** FIND documents. */
    public Map<String, Object> find(String block, FindOptions opts, String db) {
        StringBuilder cmd = new StringBuilder("FIND ").append(block);
        if (opts != null) {
            if (opts.select != null && !opts.select.isEmpty()) {
                cmd.append(" SELECT ").append(String.join(", ", opts.select));
            }
            if (opts.where != null) {
                cmd.append(" WHERE ").append(opts.where);
            }
            if (opts.order != null) {
                cmd.append(" ORDER ").append(opts.order);
            }
            if (opts.limit != null) {
                cmd.append(" LIMIT ").append(opts.limit);
            }
            if (opts.offset != null) {
                cmd.append(" OFFSET ").append(opts.offset);
            }
        }
        return query(cmd.toString(), db);
    }

    public Map<String, Object> find(String block, FindOptions opts) {
        return find(block, opts, null);
    }

    /** SEARCH full-text. */
    public Map<String, Object> search(String block, String text, boolean exact, boolean fuzzy,
                                       boolean withScore, boolean withMatches, String db) {
        StringBuilder cmd = new StringBuilder("SEARCH ").append(block).append(' ').append(Json.encode(text));
        if (exact) {
            cmd.append(" EXACT");
        }
        if (fuzzy) {
            cmd.append(" FUZZY");
        }
        if (withScore) {
            cmd.append(" WITH SCORE");
        }
        if (withMatches) {
            cmd.append(" WITH MATCHES");
        }
        return query(cmd.toString(), db);
    }

    /** UPDATE documents matching a WHERE clause with a raw SET/INC/PUSH clause. */
    public Map<String, Object> update(String block, String where, String setClause, String db) {
        return query("UPDATE " + block + " WHERE " + where + " " + setClause, db);
    }

    public Map<String, Object> update(String block, String where, String setClause) {
        return update(block, where, setClause, null);
    }

    /** DELETE documents matching a WHERE clause. */
    public Map<String, Object> delete(String block, String where, String db) {
        return query("DELETE " + block + " WHERE " + where, db);
    }

    public Map<String, Object> delete(String block, String where) {
        return delete(block, where, null);
    }

    /** COUNT documents matching an optional WHERE clause (null means "no filter"). */
    public Map<String, Object> count(String block, String where, String db) {
        String cmd = (where != null) ? ("COUNT " + block + " WHERE " + where) : ("COUNT " + block);
        return query(cmd, db);
    }

    public Map<String, Object> health() {
        return request("GET", queryUrl + "/health", null);
    }

    public Map<String, Object> status() {
        return request("GET", queryUrl + "/status", null);
    }

    /**
     * Subscribes to the real-time change stream (Server-Sent Events)
     * and blocks the calling thread, invoking {@code onEvent} for each
     * {@code change} event (a Map with keys op/db/block/id/data/timestamp).
     *
     * <p>Returns when the connection is closed. Run in a background
     * thread if you don't want to block the caller.
     */
    @SuppressWarnings("unchecked")
    public void watch(Consumer<Map<String, Object>> onEvent, String db, String block) throws Exception {
        StringBuilder url = new StringBuilder(queryUrl).append("/watch");
        String sep = "?";
        if (db != null) {
            url.append(sep).append("db=").append(urlEncode(db));
            sep = "&";
        }
        if (block != null) {
            url.append(sep).append("block=").append(urlEncode(block));
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url.toString())).GET();
        authorized(builder);

        HttpResponse<java.io.InputStream> response =
                http.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("data:")) {
                    String payload = line.substring(5).trim();
                    Object decoded = Json.decode(payload);
                    if (decoded instanceof Map) {
                        onEvent.accept((Map<String, Object>) decoded);
                    }
                }
            }
        }
    }

    public void watch(Consumer<Map<String, Object>> onEvent) throws Exception {
        watch(onEvent, null, null);
    }
}
