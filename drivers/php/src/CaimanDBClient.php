<?php

declare(strict_types=1);

namespace CaimanDB;

/**
 * CaimanDB driver for PHP.
 *
 * Talks to CaimanDB's query HTTP server (default port 1555) and,
 * optionally, its admin HTTP server (default port 1556) for login.
 * Requires only the `curl` extension (bundled with most PHP builds).
 *
 * Wire protocol (see docs/api/http-api.md in the CaimanDB repo):
 *   POST {queryUrl}/query?query=<NQL>&db=<db>   -> {success, result, db}
 *   GET  {queryUrl}/health
 *   GET  {queryUrl}/status
 *   GET  {queryUrl}/watch?db=&block=            -> Server-Sent Events
 *   POST {adminUrl}/api/v1/auth/login  {username, password}
 *                                                -> {token, user, role, expires}
 *
 * Auth: either HTTP Basic (username/password) or a JWT Bearer token
 * obtained via login() or supplied directly.
 */
class CaimanDBException extends \RuntimeException
{
    /** @var int */
    public $status;
    /** @var array<string, mixed> */
    public $body;

    /**
     * @param array<string, mixed> $body
     */
    public function __construct(string $message, int $status = 0, array $body = [])
    {
        parent::__construct($message);
        $this->status = $status;
        $this->body = $body;
    }
}

class CaimanDBClient
{
    private string $queryUrl;
    private string $adminUrl;
    private ?string $username;
    private ?string $password;
    private ?string $token;
    private string $defaultDb;
    private int $timeout;

    public function __construct(
        string $queryUrl = 'http://localhost:1555',
        string $adminUrl = 'http://localhost:1556',
        ?string $username = null,
        ?string $password = null,
        ?string $token = null,
        string $db = 'default',
        int $timeout = 30
    ) {
        $this->queryUrl = rtrim($queryUrl, '/');
        $this->adminUrl = rtrim($adminUrl, '/');
        $this->username = $username;
        $this->password = $password;
        $this->token = $token;
        $this->defaultDb = $db;
        $this->timeout = $timeout;
    }

    /**
     * @return array<int, string>
     */
    private function authHeaders(): array
    {
        if ($this->token !== null && $this->token !== '') {
            return ['Authorization: Bearer ' . $this->token];
        }
        if ($this->username !== null) {
            $encoded = base64_encode($this->username . ':' . ($this->password ?? ''));
            return ['Authorization: Basic ' . $encoded];
        }
        return [];
    }

    /**
     * @param array<string, mixed>|null $jsonBody
     * @param array<int, string> $extraHeaders
     * @return array<string, mixed>
     */
    private function request(string $method, string $url, ?array $jsonBody = null, array $extraHeaders = []): array
    {
        $ch = curl_init($url);
        if ($ch === false) {
            throw new CaimanDBException('failed to initialize cURL handle');
        }

        $headers = array_merge($this->authHeaders(), $extraHeaders);
        $body = null;
        if ($jsonBody !== null) {
            $body = json_encode($jsonBody);
            $headers[] = 'Content-Type: application/json';
        }

        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, $this->timeout);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }

        $raw = curl_exec($ch);
        if ($raw === false) {
            $error = curl_error($ch);
            curl_close($ch);
            throw new CaimanDBException('request failed: ' . $error);
        }
        $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        $parsed = [];
        if ($raw !== '') {
            $decoded = json_decode($raw, true);
            $parsed = is_array($decoded) ? $decoded : ['raw' => $raw];
        }

        if ($status >= 400) {
            $message = $parsed['error'] ?? $parsed['result'] ?? ('request failed (' . $status . ')');
            throw new CaimanDBException((string) $message, $status, $parsed);
        }
        return $parsed;
    }

    /**
     * Logs in against the admin server and stores the JWT for
     * subsequent requests.
     *
     * @return array<string, mixed> {token, user, role, expires}
     */
    public function login(string $username, string $password): array
    {
        $body = $this->request('POST', $this->adminUrl . '/api/v1/auth/login', [
            'username' => $username,
            'password' => $password,
        ]);
        $this->token = $body['token'] ?? null;
        $this->username = $username;
        return $body;
    }

    /**
     * Executes a raw NQL command, e.g. 'FIND users WHERE age > 18'.
     *
     * @return array<string, mixed>
     */
    public function query(string $nql, ?string $db = null): array
    {
        $params = http_build_query([
            'query' => $nql,
            'db' => $db ?? $this->defaultDb,
        ]);
        $body = $this->request('POST', $this->queryUrl . '/query?' . $params);
        if (isset($body['success']) && $body['success'] === false) {
            throw new CaimanDBException((string) ($body['result'] ?? 'query failed'), 0, $body);
        }
        return $body;
    }

    // ---- Convenience wrappers over query() -------------------------------

    /**
     * INSERT one document (assoc array) or several (array of assoc arrays).
     *
     * @param array<string, mixed>|array<int, array<string, mixed>> $docOrDocs
     * @return array<string, mixed>
     */
    public function insert(string $block, array $docOrDocs, ?string $db = null): array
    {
        $payload = json_encode($docOrDocs);
        return $this->query(sprintf('INSERT %s %s', $block, $payload), $db);
    }

    /**
     * GET a single document by id.
     *
     * @return array<string, mixed>
     */
    public function get(string $block, string $id, ?string $db = null): array
    {
        return $this->query(sprintf('GET %s %s', $block, $id), $db);
    }

    /**
     * FIND documents.
     *
     * @param array<int, string>|null $select
     * @return array<string, mixed>
     */
    public function find(
        string $block,
        ?string $where = null,
        ?array $select = null,
        ?string $order = null,
        ?int $limit = null,
        ?int $offset = null,
        ?string $db = null
    ): array {
        $cmd = 'FIND ' . $block;
        if ($select !== null && count($select) > 0) {
            $cmd .= ' SELECT ' . implode(', ', $select);
        }
        if ($where !== null) {
            $cmd .= ' WHERE ' . $where;
        }
        if ($order !== null) {
            $cmd .= ' ORDER ' . $order;
        }
        if ($limit !== null) {
            $cmd .= ' LIMIT ' . $limit;
        }
        if ($offset !== null) {
            $cmd .= ' OFFSET ' . $offset;
        }
        return $this->query($cmd, $db);
    }

    /**
     * SEARCH full-text.
     *
     * @return array<string, mixed>
     */
    public function search(
        string $block,
        string $text,
        bool $exact = false,
        bool $fuzzy = false,
        bool $withScore = false,
        bool $withMatches = false,
        ?string $db = null
    ): array {
        $cmd = sprintf('SEARCH %s %s', $block, json_encode($text));
        if ($exact) {
            $cmd .= ' EXACT';
        }
        if ($fuzzy) {
            $cmd .= ' FUZZY';
        }
        if ($withScore) {
            $cmd .= ' WITH SCORE';
        }
        if ($withMatches) {
            $cmd .= ' WITH MATCHES';
        }
        return $this->query($cmd, $db);
    }

    /**
     * UPDATE documents matching a WHERE clause with a raw SET/INC/PUSH clause.
     *
     * @return array<string, mixed>
     */
    public function update(string $block, string $where, string $setClause, ?string $db = null): array
    {
        return $this->query(sprintf('UPDATE %s WHERE %s %s', $block, $where, $setClause), $db);
    }

    /**
     * DELETE documents matching a WHERE clause.
     *
     * @return array<string, mixed>
     */
    public function delete(string $block, string $where, ?string $db = null): array
    {
        return $this->query(sprintf('DELETE %s WHERE %s', $block, $where), $db);
    }

    /**
     * COUNT documents matching an optional WHERE clause.
     *
     * @return array<string, mixed>
     */
    public function count(string $block, ?string $where = null, ?string $db = null): array
    {
        $cmd = $where !== null ? sprintf('COUNT %s WHERE %s', $block, $where) : sprintf('COUNT %s', $block);
        return $this->query($cmd, $db);
    }

    /**
     * @return array<string, mixed>
     */
    public function health(): array
    {
        return $this->request('GET', $this->queryUrl . '/health');
    }

    /**
     * @return array<string, mixed>
     */
    public function status(): array
    {
        return $this->request('GET', $this->queryUrl . '/status');
    }

    /**
     * Subscribes to the real-time change stream (Server-Sent Events)
     * and blocks the calling process, invoking $onEvent for each
     * `change` event: ['op', 'db', 'block', 'id', 'data', 'timestamp'].
     *
     * Stops when the connection is closed or $onEvent returns false.
     *
     * @param callable(array<string, mixed>): (bool|void) $onEvent
     */
    public function watch(callable $onEvent, ?string $db = null, ?string $block = null): void
    {
        $params = [];
        if ($db !== null) {
            $params['db'] = $db;
        }
        if ($block !== null) {
            $params['block'] = $block;
        }
        $qs = count($params) > 0 ? ('?' . http_build_query($params)) : '';
        $url = $this->queryUrl . '/watch' . $qs;

        $ch = curl_init($url);
        if ($ch === false) {
            throw new CaimanDBException('failed to initialize cURL handle');
        }

        $buffer = '';
        $stopped = false;
        $writeCallback = function ($curlHandle, string $chunk) use (&$buffer, $onEvent, &$stopped): int {
            $buffer .= $chunk;
            while (($pos = strpos($buffer, "\n\n")) !== false) {
                $rawEvent = substr($buffer, 0, $pos);
                $buffer = substr($buffer, $pos + 2);
                foreach (explode("\n", $rawEvent) as $line) {
                    if (strncmp($line, 'data:', 5) === 0) {
                        $payload = trim(substr($line, 5));
                        $decoded = json_decode($payload, true);
                        if (is_array($decoded)) {
                            if ($onEvent($decoded) === false) {
                                $stopped = true;
                            }
                        }
                    }
                }
            }
            return $stopped ? 0 : strlen($chunk);
        };

        curl_setopt($ch, CURLOPT_HTTPHEADER, $this->authHeaders());
        curl_setopt($ch, CURLOPT_TIMEOUT, 0);
        curl_setopt($ch, CURLOPT_WRITEFUNCTION, $writeCallback);
        curl_exec($ch);
        curl_close($ch);
    }
}
