// Package caimandb is a dependency-free Go client for CaimanDB.
//
// It talks to CaimanDB's query HTTP server (default port 1555) and,
// optionally, its admin HTTP server (default port 1556) for login.
// Only the standard library is used.
//
// Wire protocol (see docs/api/http-api.md in the CaimanDB repo):
//
//	POST {QueryURL}/query?query=<NQL>&db=<db>   -> {success, result, db}
//	GET  {QueryURL}/health
//	GET  {QueryURL}/status
//	GET  {QueryURL}/watch?db=&block=            -> Server-Sent Events
//	POST {AdminURL}/api/v1/auth/login  {username, password}
//	                                             -> {token, user, role, expires}
//
// Auth: either HTTP Basic (Username/Password) or a JWT Bearer token
// obtained via Login or set directly on Token.
package caimandb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Error is returned for any non-2xx response or {"success": false} body.
type Error struct {
	Message string
	Status  int
	Body    map[string]any
}

func (e *Error) Error() string {
	return fmt.Sprintf("caimandb: %s (status %d)", e.Message, e.Status)
}

// Client is a CaimanDB client. Construct with New.
type Client struct {
	QueryURL string
	AdminURL string
	Username string
	Password string
	Token    string
	DefaultDB string

	HTTPClient *http.Client
}

// Options configures a new Client.
type Options struct {
	QueryURL string // default "http://localhost:1555"
	AdminURL string // default "http://localhost:1556"
	Username string
	Password string
	Token    string
	DB       string // default "default"
	Timeout  time.Duration
}

// New creates a Client from Options. Any zero-value fields fall back
// to sane defaults (localhost, default db, 30s timeout).
func New(opts Options) *Client {
	queryURL := opts.QueryURL
	if queryURL == "" {
		queryURL = "http://localhost:1555"
	}
	adminURL := opts.AdminURL
	if adminURL == "" {
		adminURL = "http://localhost:1556"
	}
	db := opts.DB
	if db == "" {
		db = "default"
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		QueryURL:   strings.TrimRight(queryURL, "/"),
		AdminURL:   strings.TrimRight(adminURL, "/"),
		Username:   opts.Username,
		Password:   opts.Password,
		Token:      opts.Token,
		DefaultDB:  db,
		HTTPClient: &http.Client{Timeout: timeout},
	}
}

func (c *Client) authHeader(req *http.Request) {
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
		return
	}
	if c.Username != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}
}

func (c *Client) do(method, rawURL string, body any) (map[string]any, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, rawURL, reqBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.authHeader(req)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var parsed map[string]any
	if len(raw) > 0 {
		if jsonErr := json.Unmarshal(raw, &parsed); jsonErr != nil {
			parsed = map[string]any{"raw": string(raw)}
		}
	}

	if resp.StatusCode >= 400 {
		msg := fmt.Sprintf("request failed (%d)", resp.StatusCode)
		if e, ok := parsed["error"].(string); ok && e != "" {
			msg = e
		} else if r, ok := parsed["result"].(string); ok && r != "" {
			msg = r
		}
		return parsed, &Error{Message: msg, Status: resp.StatusCode, Body: parsed}
	}
	return parsed, nil
}

// Login authenticates against the admin server and stores the
// returned JWT on c.Token for subsequent requests.
func (c *Client) Login(username, password string) (map[string]any, error) {
	body, err := c.do("POST", c.AdminURL+"/api/v1/auth/login", map[string]string{
		"username": username,
		"password": password,
	})
	if err != nil {
		return body, err
	}
	if token, ok := body["token"].(string); ok {
		c.Token = token
	}
	c.Username = username
	return body, nil // {token, user, role, expires}
}

// Query executes a raw NQL command, e.g. `FIND users WHERE age > 18`.
// db defaults to c.DefaultDB when empty.
func (c *Client) Query(nql, db string) (map[string]any, error) {
	if db == "" {
		db = c.DefaultDB
	}
	q := url.Values{}
	q.Set("query", nql)
	q.Set("db", db)
	body, err := c.do("POST", c.QueryURL+"/query?"+q.Encode(), nil)
	if err != nil {
		return body, err
	}
	if success, ok := body["success"].(bool); ok && !success {
		result, _ := body["result"].(string)
		return body, &Error{Message: result, Body: body}
	}
	return body, nil
}

// FindOptions configures Find.
type FindOptions struct {
	Where  string   // raw WHERE clause, e.g. `age > 18 AND status = "active"`
	Select []string // field names to project
	Order  string   // e.g. "age:DESC"
	Limit  int      // 0 means "not set"
	Offset int
}

// Insert inserts one document (map/struct) or several (a slice) into block.
func (c *Client) Insert(block string, docOrDocs any, db string) (map[string]any, error) {
	payload, err := json.Marshal(docOrDocs)
	if err != nil {
		return nil, err
	}
	return c.Query(fmt.Sprintf("INSERT %s %s", block, payload), db)
}

// Get fetches a single document by id.
func (c *Client) Get(block, id, db string) (map[string]any, error) {
	return c.Query(fmt.Sprintf("GET %s %s", block, id), db)
}

// Find runs a FIND query built from opts.
func (c *Client) Find(block string, opts FindOptions, db string) (map[string]any, error) {
	cmd := "FIND " + block
	if len(opts.Select) > 0 {
		cmd += " SELECT " + strings.Join(opts.Select, ", ")
	}
	if opts.Where != "" {
		cmd += " WHERE " + opts.Where
	}
	if opts.Order != "" {
		cmd += " ORDER " + opts.Order
	}
	if opts.Limit > 0 {
		cmd += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		cmd += fmt.Sprintf(" OFFSET %d", opts.Offset)
	}
	return c.Query(cmd, db)
}

// SearchOptions configures Search.
type SearchOptions struct {
	Exact       bool
	Fuzzy       bool
	WithScore   bool
	WithMatches bool
}

// Search runs a full-text SEARCH query.
func (c *Client) Search(block, text string, opts SearchOptions, db string) (map[string]any, error) {
	payload, err := json.Marshal(text)
	if err != nil {
		return nil, err
	}
	cmd := fmt.Sprintf("SEARCH %s %s", block, payload)
	if opts.Exact {
		cmd += " EXACT"
	}
	if opts.Fuzzy {
		cmd += " FUZZY"
	}
	if opts.WithScore {
		cmd += " WITH SCORE"
	}
	if opts.WithMatches {
		cmd += " WITH MATCHES"
	}
	return c.Query(cmd, db)
}

// Update updates documents matching where with a raw SET/INC/PUSH clause.
func (c *Client) Update(block, where, setClause, db string) (map[string]any, error) {
	return c.Query(fmt.Sprintf("UPDATE %s WHERE %s %s", block, where, setClause), db)
}

// Delete deletes documents matching where.
func (c *Client) Delete(block, where, db string) (map[string]any, error) {
	return c.Query(fmt.Sprintf("DELETE %s WHERE %s", block, where), db)
}

// Count counts documents matching an optional where clause (pass "" for none).
func (c *Client) Count(block, where, db string) (map[string]any, error) {
	if where == "" {
		return c.Query(fmt.Sprintf("COUNT %s", block), db)
	}
	return c.Query(fmt.Sprintf("COUNT %s WHERE %s", block, where), db)
}

// Health checks GET /health.
func (c *Client) Health() (map[string]any, error) {
	return c.do("GET", c.QueryURL+"/health", nil)
}

// Status checks GET /status.
func (c *Client) Status() (map[string]any, error) {
	return c.do("GET", c.QueryURL+"/status", nil)
}

// ChangeEvent is one event from the real-time change stream.
type ChangeEvent struct {
	Op        string         `json:"op"`
	DB        string         `json:"db"`
	Block     string         `json:"block"`
	ID        string         `json:"id"`
	Data      map[string]any `json:"data"`
	Timestamp int64          `json:"timestamp"`
}

// Watch subscribes to the real-time change stream (Server-Sent Events)
// and blocks the calling goroutine, invoking onEvent for each change.
// db and block are optional filters ("" means "no filter"). Watch
// returns when the connection is closed or the request's context
// (via WatchContext) is canceled.
func (c *Client) Watch(onEvent func(ChangeEvent), db, block string) error {
	q := url.Values{}
	if db != "" {
		q.Set("db", db)
	}
	if block != "" {
		q.Set("block", block)
	}
	rawURL := c.QueryURL + "/watch"
	if enc := q.Encode(); enc != "" {
		rawURL += "?" + enc
	}

	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return err
	}
	c.authHeader(req)

	// SSE streams stay open indefinitely; don't apply the client's
	// normal request timeout here.
	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // heartbeat comment or blank line
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		var ev ChangeEvent
		if jsonErr := json.Unmarshal([]byte(payload), &ev); jsonErr == nil {
			onEvent(ev)
		}
	}
	return scanner.Err()
}
