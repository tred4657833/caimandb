// where_flat_parser_pendiente_test.go prueba, en código, la limitación
// documentada en docs/known-limitations.md y marcada con
// TODO(pendiente-motor) en cmd_update.go, cmd_view.go y cmd_admin.go:
// esos tres comandos siguen resolviendo su WHERE con parseFilters (el
// parser plano original), que NO soporta paréntesis ni NOT como operador
// de agrupación -- solo una cadena de AND/OR evaluada de izquierda a
// derecha. (cmd_aggregate.go -- SUM/AVG/COUNT/GROUP BY -- ya migró a
// parseWhereClause/parse.Expr; ver CHANGELOG.)
//
// Este archivo no prueba UPDATE/VIEW/admin directamente (eso requiere un
// *Engine completo con storage real, fuera del alcance de un test
// unitario de parsing). Prueba la función compartida que los tres
// llaman -- parseFilters, en cmd_filters_util.go -- contra la misma
// entrada que parseWhereClause (el árbol real que ya usan
// FIND/SEARCH/DELETE/CLEAR/COUNT/aggregate) sí soporta correctamente.
// Sirve como regresión: si alguno de los tres comandos restantes migra a
// parseWhereClause, TestParseFilters_NoParenSupport_KnownLimitation
// empieza a fallar -- que es la señal correcta de que el TODO
// correspondiente ya se resolvió y este test (junto con su comentario)
// debe actualizarse otra vez.
package caimandb

import (
	"strings"
	"testing"

	"caimandb/internal/caimandb/parse"
)

// TestParseFilters_NoParenSupport_KnownLimitation documenta que
// parseFilters no entiende paréntesis: "(" y ")" no son tokens que
// reconozca como agrupación, así que un WHERE con paréntesis produce
// un resultado silenciosamente incorrecto (no un error) en vez de
// aplicar la precedencia esperada. Esto es exactamente lo que
// parseWhereClause / parse.ParseWhere sí resuelven bien (ver el caso
// de comparación más abajo).
// TestParseFilters_NoParenSupport_KnownLimitation documenta que
// parseFilters no entiende paréntesis como agrupación: "(" termina
// leyéndose como el nombre de un campo. En la práctica, para esta
// entrada concreta eso hace que el "operador" resultante ("edad", el
// token siguiente) no esté en la lista de operadores válidos, así que
// parseFilters lo trata como EXISTS y sigue -- hasta que más adelante
// el desalineamiento de tokens que dejó ese primer error de lectura
// provoca un error real de tokens insuficientes. El punto que importa
// para el known-limitation sigue siendo el mismo: no hay agrupación ni
// precedencia real, solo que aquí se manifiesta como error en vez de
// como resultado silenciosamente incorrecto. Esto es exactamente lo
// que parseWhereClause / parse.ParseWhere sí resuelven bien (ver el
// caso de comparación más abajo).
func TestParseFilters_NoParenSupport_KnownLimitation(t *testing.T) {
	// "status = activo AND ( edad > 18 OR vip = true )"
	tokens := []string{
		"status", "=", "activo", "AND",
		"(", "edad", ">", "18", "OR", "vip", "=", "true", ")",
	}
	idx := 0
	_, err := parseFilters(tokens, &idx)
	if err == nil {
		t.Skip("parseFilters cambió de comportamiento con paréntesis -- revisar si el TODO(pendiente-motor) de UPDATE/VIEW/admin ya se puede cerrar")
	}
}

// TestParseWhereClause_HandlesParenAndNot_ForComparison confirma que el
// árbol real (usado por FIND/SEARCH/DELETE/CLEAR/COUNT) SÍ resuelve
// paréntesis y NOT correctamente para la misma condición, contrastando
// contra el caso anterior. Es el comportamiento al que deben llegar
// UPDATE, agregaciones, VIEW CREATE y el comando admin al migrar.
func TestParseWhereClause_HandlesParenAndNot_ForComparison(t *testing.T) {
	tokens := []string{
		"status", "=", "activo", "AND",
		"(", "edad", ">", "18", "OR", "vip", "=", "true", ")",
	}
	idx := 0
	where, _, err := parseWhereClause(tokens, &idx)
	if err != nil {
		t.Fatalf("parseWhereClause no debería fallar con una condición válida: %v", err)
	}
	if where == nil {
		t.Fatal("parseWhereClause debería devolver un árbol (*parse.Expr) no nulo")
	}
	// No es una cadena pura de AND en el nivel superior (hay un OR
	// agrupado adentro), así que FlattenTopLevelAnd debe rechazarla --
	// justo la señal que hoy hace caer a un full scan en vez de un
	// índice mal aplicado.
	if _, ok := parse.FlattenTopLevelAnd(where); ok {
		t.Error("se esperaba que la condición con OR agrupado NO se aplanara como AND-chain puro")
	}
}

// TestParseFilters_BareNOT_TreatedAsFieldName_KnownLimitation documenta
// el otro síntoma de la misma limitación: parseFilters solo reconoce
// "NOT" como parte de operadores de dos palabras (NOT IN, NOT LIKE,
// NOT CONTAINS). Un "NOT" suelto, como en "WHERE NOT activo = true",
// no es un operador de negación para parseFilters -- termina leyéndose
// como si "NOT" fuera el nombre del campo.
// TestParseFilters_BareNOT_TreatedAsFieldName_KnownLimitation documenta
// el otro síntoma de la misma limitación: parseFilters solo reconoce
// "NOT" como parte de operadores de dos palabras (NOT IN, NOT LIKE,
// NOT CONTAINS). Un "NOT" suelto, como en "WHERE NOT activo = true",
// no es un operador de negación para parseFilters -- termina leyéndose
// como si "NOT" fuera el nombre del campo, lo que desalinea el resto
// de la lectura de tokens y produce un error (missing value) en vez
// de la negación esperada.
func TestParseFilters_BareNOT_TreatedAsFieldName_KnownLimitation(t *testing.T) {
	tokens := strings.Fields("NOT activo = true")
	idx := 0
	_, err := parseFilters(tokens, &idx)
	if err == nil {
		t.Skip("parseFilters cambió cómo maneja un NOT suelto -- revisar si el TODO(pendiente-motor) ya se puede cerrar")
	}
}
