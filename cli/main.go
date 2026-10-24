// Command terminal is CaimanDB's single-file CLI client: a raw-mode
// interactive shell with its own line editor, cursor movement, and history.
//
// First run asks for the port (1555 = query, 1556 = admin), the admin
// user and the password, then saves them to conf/cli.json so the next
// run skips straight to the prompt. Saved settings are edited later
// without leaving the shell:
//
//	--modif port: 1556, admin: juan, pass: 123456
//
// --modif ONLY writes conf/cli.json (and updates the values used for
// the rest of this session) -- it never sends a command to the server.
//
// Everything else typed at the prompt is sent as-is to CaimanDB's
// /query HTTP endpoint and the response is printed to the terminal,
// with the line itself colorized as reserved words of the query
// language are typed.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/term"
)

// ============================================
// CONFIG (conf/cli.json)
// ============================================

const configPath = "conf/cli.json"

type cliConfig struct {
	Port  int    `json:"port"`
	Admin string `json:"admin"`
	Pass  string `json:"pass"`
}

func loadConfig() (cliConfig, error) {
	var cfg cliConfig
	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Port != 1555 && cfg.Port != 1556 {
		return cfg, fmt.Errorf("invalid saved port: %d", cfg.Port)
	}
	return cfg, nil
}

func saveConfig(cfg cliConfig) error {
	if err := os.MkdirAll(filepath.Dir(configPath), 0750); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath, data, 0600)
}

// askConfig prompts once, in normal (cooked) terminal mode -- before
// the shell switches to raw mode -- for the three values the client
// needs: the port, the admin user, and the password (hidden, via
// term.ReadPassword, the same way app.go already asks for one).
func askConfig(reader *bufio.Reader) cliConfig {
	var cfg cliConfig

	for {
		fmt.Print(colYellow + "Port (1555/1556): " + colReset)
		raw, _ := reader.ReadString('\n')
		p, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || (p != 1555 && p != 1556) {
			fmt.Println(colRed + "Invalid port, use 1555 or 1556" + colReset)
			continue
		}
		cfg.Port = p
		break
	}

	fmt.Print(colYellow + "Admin: " + colReset)
	admin, _ := reader.ReadString('\n')
	cfg.Admin = strings.TrimSpace(admin)

	fmt.Print(colYellow + "Password: " + colReset)
	passBytes, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err == nil {
		cfg.Pass = string(passBytes)
	} else {
		// Fallback for a stdin that isn't a real terminal (piped input).
		pass, _ := reader.ReadString('\n')
		cfg.Pass = strings.TrimSpace(pass)
	}

	return cfg
}

// applyModif parses "--modif key: value, key2: value2, ..." and
// updates ONLY the matching fields, in memory and in conf/cli.json.
// It never issues a query against the server.
func applyModif(cfg *cliConfig, line string) string {
	rest := strings.TrimSpace(strings.TrimPrefix(line, "--modif"))
	if rest == "" {
		return colRed + "Usage: --modif port: 1555, admin: username, pass: secret" + colReset
	}

	var changed []string
	for _, pair := range strings.Split(rest, ",") {
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		val := strings.TrimSpace(kv[1])

		switch key {
		case "port":
			p, err := strconv.Atoi(val)
			if err != nil || (p != 1555 && p != 1556) {
				return colRed + "Invalid port (use 1555 or 1556), nothing modified" + colReset
			}
			cfg.Port = p
			changed = append(changed, "port")
		case "admin":
			cfg.Admin = val
			changed = append(changed, "admin")
		case "pass":
			cfg.Pass = val
			changed = append(changed, "pass")
		default:
			return colRed + "Unknown field: " + key + colReset
		}
	}

	if len(changed) == 0 {
		return colYellow + "Nothing to modify." + colReset
	}
	if err := saveConfig(*cfg); err != nil {
		return colRed + "Failed to save " + configPath + ": " + err.Error() + colReset
	}
	return colGreen + configPath + " updated (" + strings.Join(changed, ", ") + ")" + colReset
}

// ============================================
// COLORS
// ============================================

const (
	colReset       = "\x1b[0m"
	colBold        = "\x1b[1m"
	colCyan        = "\x1b[36m"
	colGreen       = "\x1b[32m"
	colYellow      = "\x1b[33m"
	colRed         = "\x1b[31m"
	colGray        = "\x1b[90m"
	colOrange      = "\x1b[38;5;208m" // Orange for :
	colBlue        = "\x1b[34m"       // Blue for field names
	colWhite       = "\x1b[37m"       // White for symbols
	colBrightGreen = "\x1b[92m"       // Bright green for string values
)

// ============================================
// RESERVED WORDS
// ============================================

// reservedWords is every command/clause keyword CaimanDB's query
// language recognizes, used to colorize the line as it's typed.
var reservedWords = map[string]bool{
	"ANALYZE": true, "AND": true, "AVG": true, "BACKUP": true, "BEGIN": true,
	"BETWEEN": true, "BLOCK": true, "BLOCKS": true, "BY": true, "CACHE": true,
	"CD": true, "CHECK": true, "CLEAR": true, "CLUSTER": true, "COMMIT": true,
	"COMPACT": true, "CONTAINS": true, "COUNT": true, "CREATE": true, "DB": true,
	"DBS": true, "DEC": true, "DELETE": true, "DESCRIBE": true, "DROP": true,
	"EXACT": true, "EXCLUDE": true, "EXISTS": true, "EXIT": true, "EXPLAIN": true,
	"EXPORT": true, "FIND": true, "FLEX": true, "FROM": true, "FUZZY": true,
	"GET": true, "GROUP": true, "HAVING": true, "HEALTH": true, "HELP": true,
	"HOT": true, "IMPORT": true, "IN": true, "INC": true, "INDEXES": true,
	"INFO": true, "INSERT": true, "IS": true, "JOIN": true, "LIKE": true,
	"LIMIT": true, "LIST": true, "LS": true, "MAX": true, "MIN": true,
	"NOT": true, "NULL": true, "OFFSET": true, "ON": true, "ONLY": true,
	"OPTIMIZE": true, "OR": true, "ORDER": true, "PING": true, "POST": true,
	"PULL": true, "PUSH": true, "PWD": true, "REBALANCE": true, "REBUILD": true,
	"RELATE": true, "RENAME": true, "REPAIR": true, "RESTORE": true,
	"ROLLBACK": true, "SCALE": true, "SEARCH": true, "SELECT": true, "SET": true,
	"SHARD": true, "SHARDS": true, "SHOW": true, "SIZE": true, "STATS": true,
	"STATUS": true, "SUM": true, "TO": true, "USE": true, "WHERE": true,
	"WITH": true,
}

// highlight colors every reserved word in line for the live prompt;
// everything else (db/block names, values, punctuation) stays plain.
// ANSI codes are zero-width on the terminal, so injecting them here
// never throws off the cursor-column math in Terminal.draw.
func highlight(line string) string {
	words := strings.Fields(line)
	for i, w := range words {
		trimmed := strings.Trim(w, "(),;")
		if trimmed != "" && reservedWords[strings.ToUpper(trimmed)] {
			words[i] = colBold + colCyan + w + colReset
		}
	}
	return strings.Join(words, " ")
}

// ============================================
// RESULT COLORIZER
// ============================================

// fieldLineRe matches a single pretty-printed JSON field line, e.g.
//
//	  "age": 22,
//	  "name": "Eve"
//
// captured as: indent, field name, value, optional trailing comma.
var fieldLineRe = regexp.MustCompile(`^(\s*)"([^"]+)"\s*:\s*(.*?)(,?)\s*$`)

// bracketLineRe matches a line that is only brace/bracket punctuation,
// e.g. "{", "}", "}," or "]," -- the open/close lines of a
// pretty-printed JSON object/array with nothing else on them.
var bracketLineRe = regexp.MustCompile(`^(\s*)([\{\}\[\]])(,?)\s*$`)

// colorizeValue colors a single JSON value token: strings bright
// green, true/false/null and numbers yellow, anything else (nested
// "{" / "[" openers, unrecognized tokens) left as-is.
func colorizeValue(v string) string {
	switch {
	case strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`) && len(v) >= 2:
		return colBrightGreen + v + colReset
	case v == "true" || v == "false" || v == "null":
		return colYellow + v + colReset
	default:
		if _, err := strconv.ParseFloat(v, 64); err == nil {
			return colYellow + v + colReset
		}
		return v
	}
}

// colorizeResult colors a server "result" string one line at a time,
// only touching lines that are unambiguously JSON (a "field": value
// line, or a lone bracket line). Every other line -- summary headers
// like "Found N documents (scanned: 1, ..., took: 7.0238ms)", or a
// "_id: ..., shard: ..., data: {" preamble -- passes through exactly
// as the server sent it. This avoids the old whole-text regex
// colorizer's failure mode, where ": <number>" inside a non-JSON
// summary line (like "took: 7.0238ms") got matched and split mid-token.
func colorizeResult(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if m := bracketLineRe.FindStringSubmatch(line); m != nil {
			indent, br, comma := m[1], m[2], m[3]
			lines[i] = indent + colWhite + br + comma + colReset
			continue
		}
		if m := fieldLineRe.FindStringSubmatch(line); m != nil {
			indent, field, value, comma := m[1], m[2], m[3], m[4]
			lines[i] = indent +
				colBlue + `"` + field + `"` + colReset +
				colOrange + ":" + colReset + " " +
				colorizeValue(value) +
				colWhite + comma + colReset
		}
		// else: leave the line exactly as-is.
	}
	return strings.Join(lines, "\n")
}

// ============================================
// HTTP CLIENT (talks to /query)
// ============================================

// runCommand sends command to CaimanDB's /query endpoint over HTTP
// Basic Auth and returns the plain-text "result" field of the
// response, ready to print.
// runCommand sends command to CaimanDB's /query endpoint over HTTP
// Basic Auth and returns the plain-text "result" field of the
// response, ready to print. dbName is sent as the "db" query
// parameter: the server builds a brand-new, request-scoped Session
// from it (see handleQuery/Session in the server), so there is no
// session or cookie that remembers a previous "USE" -- every request
// has to state which database it's running against on its own.
func runCommand(client *http.Client, cfg cliConfig, dbName, command string) string {
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/query", cfg.Port)

	q := url.Values{}
	q.Set("query", command)
	if dbName != "" {
		q.Set("db", dbName)
	}

	req, err := http.NewRequest(http.MethodGet, endpoint+"?"+q.Encode(), nil)
	if err != nil {
		return colRed + "ERROR: " + err.Error() + colReset
	}
	req.SetBasicAuth(cfg.Admin, cfg.Pass)

	resp, err := client.Do(req)
	if err != nil {
		return colRed + fmt.Sprintf("Failed to connect to 127.0.0.1:%d: %v", cfg.Port, err) + colReset
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return colRed + strings.TrimSpace(string(body)) + colReset
	}

	var out struct {
		Success bool   `json:"success"`
		Result  string `json:"result"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		// Not JSON at all -- show it as-is.
		return string(body)
	}
	if !out.Success {
		return colRed + out.Result + colReset
	}

	// Color only the lines that are genuinely JSON fields/brackets;
	// summary lines like "Found N documents (scanned: 1, ..., took:
	// 9.5874ms)" and preambles like "_id: ..., shard: ..., data: {"
	// pass through untouched -- see colorizeResult's doc comment.
	return colorizeResult(out.Result)
}

// ============================================
// INTERACTIVE TERMINAL
// ============================================

type Terminal struct {
	line      string
	cursorPos int
	history   []string
	histIndex int
	cfg       *cliConfig
	client    *http.Client // shared client, reused for connection pooling (db context now travels via the ?db= param on each request, not via any session)
	running   bool
	oldState  *term.State
	rawOK     bool
	currentDB string // Current database
}

func NewTerminal(cfg *cliConfig) *Terminal {
	return &Terminal{
		history:   []string{},
		histIndex: -1,
		cfg:       cfg,
		client:    &http.Client{Timeout: 15 * time.Second},
		running:   true,
		currentDB: "default",
	}
}

func (t *Terminal) ShowBanner() {
	fmt.Print("\033[2J\033[H")
	fmt.Println(colBold + colCyan + "Welcome to CaimanDB CLI v0.0.1" + colReset)
}

func (t *Terminal) prompt() string {
	if t.currentDB != "" && t.currentDB != "default" {
		return fmt.Sprintf("caimandb[%d]/%s/> ", t.cfg.Port, t.currentDB)
	}
	return fmt.Sprintf("caimandb[%d]> ", t.cfg.Port)
}

// draw redraws the current line in place: clear the row, print the
// prompt + the highlighted line, then move the real cursor back to
// where it belongs in the (uncolored) character count.
func (t *Terminal) draw() {
	p := t.prompt()
	fmt.Printf("\r\033[K%s%s%s%s", colGreen, p, colReset, highlight(t.line))
	fmt.Printf("\033[%dG", len(p)+1+t.cursorPos)
}

// out prints server/local output while in raw mode, where the tty
// driver's normal \n -> \r\n translation is disabled, so every
// newline needs an explicit \r or multi-line results (SHOW BLOCKS,
// DESCRIBE, ...) render stair-stepped.
func (t *Terminal) out(s string) {
	fmt.Print(strings.ReplaceAll(s, "\n", "\r\n") + "\r\n")
}

func (t *Terminal) Run() {
	t.ShowBanner()

	if oldState, err := term.MakeRaw(int(os.Stdin.Fd())); err != nil {
		fmt.Println(colRed + "[!] Raw mode not available: " + err.Error() + colReset)
	} else {
		t.oldState = oldState
		t.rawOK = true
	}
	if t.rawOK {
		defer t.restore()
	}

	buffer := make([]byte, 1024)

	for t.running {
		t.draw()

		n, err := os.Stdin.Read(buffer)
		if err != nil || n == 0 {
			continue
		}
		t.processInput(buffer[:n])
	}
}

func (t *Terminal) restore() {
	if t.rawOK && t.oldState != nil {
		term.Restore(int(os.Stdin.Fd()), t.oldState)
	}
}

func (t *Terminal) processInput(data []byte) {
	for i := 0; i < len(data); i++ {
		b := data[i]

		switch b {
		case 13: // Enter
			fmt.Print("\r\n")
			t.executeCommand()

		case 127, 8: // Backspace
			if t.cursorPos > 0 {
				t.line = t.line[:t.cursorPos-1] + t.line[t.cursorPos:]
				t.cursorPos--
			}

		case 27: // Escape: arrows
			if i+2 < len(data) && data[i+1] == 91 {
				switch data[i+2] {
				case 68: // Left
					if t.cursorPos > 0 {
						t.cursorPos--
					}
				case 67: // Right
					if t.cursorPos < len(t.line) {
						t.cursorPos++
					}
				case 65: // Up: history backward
					if len(t.history) > 0 {
						if t.histIndex < 0 {
							t.histIndex = len(t.history) - 1
						} else if t.histIndex > 0 {
							t.histIndex--
						}
						t.line = t.history[t.histIndex]
						t.cursorPos = len(t.line)
					}
				case 66: // Down: history forward
					if t.histIndex >= 0 {
						t.histIndex--
						if t.histIndex >= 0 {
							t.line = t.history[t.histIndex]
						} else {
							t.line = ""
						}
						t.cursorPos = len(t.line)
					}
				}
				i += 2
			}

		case 3: // Ctrl+C
			t.quit()

		case 4: // Ctrl+D
			if t.line == "" {
				t.quit()
			}

		case 12: // Ctrl+L
			t.ShowBanner()

		default:
			if b >= 32 && b <= 126 {
				t.line = t.line[:t.cursorPos] + string(b) + t.line[t.cursorPos:]
				t.cursorPos++
			}
		}
	}
}

func (t *Terminal) executeCommand() {
	cmdLine := strings.TrimSpace(t.line)

	if cmdLine != "" && (len(t.history) == 0 || t.history[len(t.history)-1] != cmdLine) {
		t.history = append(t.history, cmdLine)
	}
	t.histIndex = -1

	switch {
	case cmdLine == "":
		// nothing to do

	case strings.EqualFold(cmdLine, "exit"), strings.EqualFold(cmdLine, "quit"):
		t.quit()

	case strings.EqualFold(cmdLine, "clear"), strings.EqualFold(cmdLine, "cls"):
		t.ShowBanner()

	case strings.HasPrefix(cmdLine, "--modif"):
		t.out(applyModif(t.cfg, cmdLine))

	case strings.EqualFold(cmdLine, "exit db"):
		t.currentDB = "default"
		t.out(colGreen + "Switched to default database" + colReset)

	default:
		// Process database commands
		upperCmd := strings.ToUpper(cmdLine)

		if strings.HasPrefix(upperCmd, "USE ") {
			dbName := strings.TrimSpace(cmdLine[4:])
			if dbName != "" {
				// Verify the database exists by trying to use it
				response := runCommand(t.client, *t.cfg, dbName, "USE "+dbName)
				if !strings.Contains(response, "error") && !strings.Contains(response, "Error") &&
					!strings.Contains(response, "does not exist") {
					t.currentDB = dbName
					t.out(colGreen + "Switched to database: " + dbName + colReset)
				} else {
					t.out(response)
				}
			}
			break
		}

		if strings.HasPrefix(upperCmd, "CREATE DB ") {
			dbName := strings.TrimSpace(cmdLine[9:])
			// First create the database
			response := runCommand(t.client, *t.cfg, "", cmdLine)
			t.out(response)
			// Check if creation was successful
			if !strings.Contains(strings.ToLower(response), "error") &&
				!strings.Contains(strings.ToLower(response), "fail") && dbName != "" {
				// Set the current database
				t.currentDB = dbName
				t.out(colGreen + "Database created and selected: " + dbName + colReset)
			}
			break
		}

		// For any other command: the server has no session state at all
		// (see handleQuery/Session server-side) -- it builds a fresh
		// Session per request straight from the "db" URL parameter. So
		// the current database has to be sent on every single request,
		// not just once via "USE".
		response := runCommand(t.client, *t.cfg, t.currentDB, cmdLine)
		if t.currentDB != "" && t.currentDB != "default" &&
			strings.Contains(strings.ToLower(response), "does not exist") {
			t.out(colRed + "Database '" + t.currentDB + "' does not exist" + colReset)
			t.currentDB = "default"
			break
		}
		t.out(response)
	}

	t.reset()
}

func (t *Terminal) reset() {
	t.line = ""
	t.cursorPos = 0
}

func (t *Terminal) quit() {
	t.restore()
	fmt.Println(colGray + "Bye." + colReset)
	os.Exit(0)
}

// ============================================
// MAIN
// ============================================

func main() {
	if runtime.GOOS == "windows" {
		fmt.Print("\x1b[0m")
	}

	cfg, err := loadConfig()
	if err != nil {
		fmt.Println(colBold + colCyan + "Welcome to CaimanDB CLI v0.0.1" + colReset)
		reader := bufio.NewReader(os.Stdin)
		cfg = askConfig(reader)
		if saveErr := saveConfig(cfg); saveErr != nil {
			fmt.Println(colRed + "Failed to save " + configPath + ": " + saveErr.Error() + colReset)
		}
	}

	terminal := NewTerminal(&cfg)
	terminal.Run()
}