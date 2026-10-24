// script_reader.go arma sentencias NQL completas a partir de líneas
// sueltas, para los tres lectores de app.go: "caimandb run <script.nql>",
// la entrada por stdin ("caimandb -") y el prompt interactivo.
//
// Antes, los tres solo sabían juntar líneas que terminaban en "\". Eso
// dejaba fuera la forma en que realmente se escriben los scripts .nql
// (y en que los documenta docs/NQL_SYNTAX.en.md): un INSERT con el JSON
// del documento indentado en varias líneas, un CREATE BLOCK con la
// lista de bloques entre paréntesis, un array de documentos... Cada
// línea de esas se ejecutaba por separado y fallaba ("unknown command
// 'name'", "Could not parse document data"), así que un script
// perfectamente válido solo funcionaba si se pegaba entero en la
// consola web.
//
// La regla ahora es la de cualquier consola SQL/JSON: la sentencia
// sigue abierta mientras queden llaves, corchetes o paréntesis sin
// cerrar (ignorando los que estén dentro de comillas), o mientras la
// línea termine en "\". Los comentarios "--" se recortan aquí, de modo
// que también se pueden poner al final de una línea con código y no
// solo en una línea propia.
package caimandb

import "strings"

// nqlInlineFlags son los tokens que empiezan con "--" y NO son un
// comentario: son modificadores de salida de FIND (ver buildFindQuery
// en cmd_find.go). Sin esta excepción, "FIND animals --type:table" se
// quedaría sin su flag al recortar el comentario.
var nqlInlineFlags = []string{"--type:", "--format:"}

// stripNQLComment devuelve line sin el comentario "--" final. Solo se
// considera comentario un "--" fuera de comillas que no inicie uno de
// los flags de nqlInlineFlags.
func stripNQLComment(line string) string {
	runes := []rune(line)
	inStr := false
	var quote rune
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		if inStr {
			if ch == quote {
				inStr = false
			}
			continue
		}
		if ch == '"' || ch == '\'' {
			inStr = true
			quote = ch
			continue
		}
		if ch == '-' && i+1 < len(runes) && runes[i+1] == '-' {
			rest := string(runes[i:])
			isFlag := false
			for _, f := range nqlInlineFlags {
				if strings.HasPrefix(rest, f) {
					isFlag = true
					break
				}
			}
			if isFlag {
				continue
			}
			return string(runes[:i])
		}
	}
	return line
}

// delimiterDelta cuenta cuántos delimitadores quedan abiertos (positivo)
// o de más cerrados (negativo) en line, ignorando lo que haya dentro de
// comillas simples o dobles.
func delimiterDelta(line string) int {
	depth := 0
	inStr := false
	var quote rune
	for _, ch := range line {
		switch {
		case inStr:
			if ch == quote {
				inStr = false
			}
		case ch == '"' || ch == '\'':
			inStr = true
			quote = ch
		case ch == '{' || ch == '[' || ch == '(':
			depth++
		case ch == '}' || ch == ']' || ch == ')':
			depth--
		}
	}
	return depth
}

// stmtBuffer acumula líneas hasta completar una sentencia.
type stmtBuffer struct {
	sb    strings.Builder
	depth int
}

// pending indica si hay una sentencia a medio escribir (para que el
// prompt interactivo muestre "...>" en vez de "> ").
func (b *stmtBuffer) pending() bool { return b.sb.Len() > 0 }

// reset descarta lo acumulado (Ctrl-C en el prompt, p. ej.).
func (b *stmtBuffer) reset() {
	b.sb.Reset()
	b.depth = 0
}

// feed agrega una línea cruda de entrada. Devuelve la sentencia completa
// y true solo cuando la sentencia terminó; mientras siga abierta,
// devuelve ("", false).
func (b *stmtBuffer) feed(raw string) (string, bool) {
	line := strings.TrimSpace(stripNQLComment(raw))
	if line == "" {
		return "", false
	}

	explicitCont := false
	if strings.HasSuffix(line, "\\") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\\"))
		explicitCont = true
	}

	if b.sb.Len() > 0 {
		b.sb.WriteByte(' ')
	}
	b.sb.WriteString(line)

	b.depth += delimiterDelta(line)
	if b.depth < 0 {
		// Más cierres que aperturas: no hay nada que esperar, y dejar
		// la profundidad en negativo haría que la siguiente sentencia
		// con una llave abierta pareciera ya cerrada.
		b.depth = 0
	}

	if explicitCont || b.depth > 0 {
		return "", false
	}

	stmt := strings.TrimSpace(b.sb.String())
	b.sb.Reset()
	b.depth = 0
	return stmt, stmt != ""
}
