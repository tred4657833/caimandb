// Package parse contiene el análisis léxico de la sintaxis NQL
// (el lenguaje de comandos de CaimanDB): partir una línea de entrada
// en tokens respetando comillas, corchetes y llaves.
//
// Este archivo vivía como tokenizer.go dentro de internal/caimandb.
// Se movió aquí porque, a diferencia del resto del paquete, no toca
// ningún tipo del motor (Engine, Document, Config, Session, Filter,
// Transaction...): solo depende de strings de la librería estándar.
// Eso lo hace el único archivo que se puede separar en su propio
// paquete sin exportar estado interno ni arriesgar ciclos de
// imports. Ver docs/known-limitations.md para el resto del análisis.
package parse

import "strings"

// Tokenize divide una línea de comando NQL en tokens, respetando
// cadenas entre comillas simples/dobles y el anidamiento de
// corchetes `[]` y llaves `{}` (para no partir arrays/objetos JSON
// embebidos en el comando).
//
// '(' y ')' fuera de comillas/corchetes/llaves siempre se emiten como
// tokens propios, incluso pegados a otro texto (p. ej. "(status" o
// "activo)"), porque son la sintaxis de agrupación del AST de WHERE
// (ver ast.go): el parser de expresiones necesita verlos como
// delimitadores separados, no como parte del nombre de un campo o de
// un valor.
func Tokenize(input string) []string {
	var tokens []string
	var buf strings.Builder
	inStr := false
	var strCh rune
	inBracket := 0
	inBrace := 0

	flush := func() {
		if buf.Len() > 0 {
			tokens = append(tokens, buf.String())
			buf.Reset()
		}
	}

	for _, ch := range input {
		switch {
		case !inStr && (ch == '"' || ch == '\''):
			inStr = true
			strCh = ch
			buf.WriteRune(ch)
		case inStr && ch == strCh:
			inStr = false
			buf.WriteRune(ch)
		case !inStr && ch == '[':
			inBracket++
			buf.WriteRune(ch)
		case !inStr && ch == ']':
			inBracket--
			buf.WriteRune(ch)
		case !inStr && ch == '{':
			inBrace++
			buf.WriteRune(ch)
		case !inStr && ch == '}':
			inBrace--
			buf.WriteRune(ch)
		case !inStr && inBracket == 0 && inBrace == 0 && (ch == '(' || ch == ')'):
			flush()
			tokens = append(tokens, string(ch))
		case !inStr && inBracket == 0 && inBrace == 0 &&
			(ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'):
			flush()
		default:
			buf.WriteRune(ch)
		}
	}
	flush()
	return tokens
}
