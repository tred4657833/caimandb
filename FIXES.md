# CaimanDB — correcciones aplicadas al motor y al `zoo.nql`

> **Importante:** este entorno no tiene toolchain de Go ni acceso a red, así que
> los cambios del motor **no se compilaron ni se ejecutaron**. Antes de usarlos:
> `go build ./... && go test ./...`.

---

## A. Bugs del motor (código Go)

Archivos nuevos: `internal/caimandb/script_reader.go`, `internal/caimandb/cmd_tx_dml.go`.
Modificados: `app.go`, `cmd_insert.go`, `cmd_update.go`, `cmd_delete.go`,
`cmd_find.go`, `cmd_admin.go`, `cmd_admin_extra.go`, `ops_export.go`.

### A1. `ROLLBACK` no deshacía nada — las transacciones no estaban conectadas al DSL

`BEGIN` creaba la transacción y la guardaba en `Session.currentTx`, pero ningún
handler la consultaba: `handleInsert` / `handleUpdate` / `handleDelete` escribían
directo contra el motor y comiteaban una por una. `TxManager.Insert/Update/Delete`
existían y no los llamaba nadie fuera de los tests. Resultado: todo lo que había
entre `BEGIN` y `ROLLBACK` quedaba persistido, justo lo contrario de lo que
documentan `help.go` ("Rollback — discard all changes") y `docs/NQL_SYNTAX.en.md`.

**Arreglo:** `cmd_tx_dml.go` añade `execInsertDoc` / `execInsertBatch` /
`execUpdate` / `execDelete`, único punto por el que esos tres comandos escriben
ahora. Si hay transacción activa ligada al mismo `<db>.<block>`, la escritura se
acumula como mutación y se aplica atómicamente en `COMMIT`; si no la hay, se
llama a la misma API de antes (el camino sin transacción no cambia).

Una sentencia dentro de `BEGIN/COMMIT` que apunte a **otro** bloque devuelve un
error explícito en vez de ejecutarse al margen — `TxManager` guarda un solo
`db+block` por transacción, y ejecutarla por fuera sería el mismo fallo silencioso
que este parche corrige.

### A2. Los scripts `.nql` multilínea no se podían ejecutar

`caimandb run script.nql`, `caimandb -` y el prompt interactivo solo unían líneas
terminadas en `\`. Un `INSERT` con el JSON indentado, un `CREATE BLOCK (...)` o un
array de documentos se ejecutaban línea por línea y fallaban
(`unknown command 'name'`, `Could not parse document data`).

**Arreglo:** `script_reader.go` introduce `stmtBuffer`, que mantiene la sentencia
abierta mientras queden `{`, `[` o `(` sin cerrar (ignorando lo que esté entre
comillas) o mientras la línea termine en `\`. Además recorta comentarios `--` al
final de una línea con código, respetando los flags `--type:` / `--format:` de
`FIND`. Los tres lectores de `app.go` lo usan.

### A3. `GET <block> @ <id>` documentado y no implementado

`handleGet` tomaba `tokens[2]`, que en esa forma es el propio `@`, así que toda
consulta con esa sintaxis respondía `Document not found: @`. Ahora se reconoce el
`@` y se quitan comillas del id.

### A4. `BACKUP <db> TO <file>` y `RESTORE <db> FROM <file>` ignoraban el archivo

Ambos handlers leían `tokens[2]` como ruta sin mirar la palabra clave, de modo que
el nombre del backup pasaba a ser literalmente `"TO"` / `"FROM"` y la ruta real se
descartaba. Ahora se acepta la palabra clave (y se mantiene la forma corta sin ella).

### A5. `TX ISOLATION` documentado y no implementado

Caía en el `default` de `handleTransaction` y devolvía el estado de la transacción.
Ahora devuelve el nivel de aislamiento configurado.

### A6. Exportación CSV corrompía objetos, arrays y nulos

`writeDocsCSV` / `Export` usaban `fmt.Sprint`, produciendo celdas como
`map[insurance_covered:false]`, `[omnivore european]` y `<nil>` — visibles hoy en
`backups/animals_export.csv`, y que al reimportar volvían como cadenas sin sentido.
El nuevo helper `csvCell` codifica en JSON compacto lo que no sea escalar y deja la
celda vacía para `nil`.

### A7. `UPDATE ... SET/INC/DEC/PUSH/PULL` corrompía valores cuando había más de un campo

Encontrado ejecutando el script completo pegado en la consola: `UPDATE animals WHERE _id = "lion001" SET name = "Simba the Great", weight_kg = 195`
dejó el nombre guardado como `Simba the Great",` — con la coma y la comilla de cierre
metidas dentro del valor — en vez de `Simba the Great`.

Causa: el tokenizer (`parse/tokenizer.go`) solo separa tokens por espacios; una coma
pegada al cierre de comillas (`"X", campo2 = ...`, el estilo que usa todo el resto del
motor — INSERT, ADD FIELD, GRANT, etc.) queda dentro del mismo token que el valor. El
parser de `INSERT key:value` divide primero por comas sobre el texto crudo y por eso no
le afecta, pero el de `UPDATE SET/INC/DEC/PUSH/PULL` trabaja sobre tokens ya partidos y
nunca le quita esa coma sobrante antes de interpretar el valor — y
`strings.Trim(valStr, "\"'")` solo quita comillas, no la coma final.

**Arreglo:** nuevo helper `stripTrailingComma`, aplicado a los 15 puntos donde estas
cinco cláusulas leen un valor (3 formas × 5 cláusulas). Cualquier `UPDATE` con más de
una asignación separada por comas se veía afectado, no solo el nombre del ejemplo.

### A8. `MEDIAN`, `MODE`, `STDDEV` documentados y no implementados

`ops_aggregate.go` solo reconocía `SUM/AVG/MIN/MAX/COUNT`; las otras tres devolvían
`ERROR: unknown aggregation: ...` pese a estar documentadas en `help.go` y en
`docs/NQL_SYNTAX.en.md`. Implementadas: `MEDIAN` y `STDDEV` (poblacional) reutilizan la
misma pasada de `Aggregate`, guardando también la lista de valores. `MODE` necesitaba su
propio método (`Engine.AggregateMode`) porque tiene sentido sobre campos no numéricos
(p. ej. `MODE animals status`), algo que la función `Aggregate` no podía devolver al
estar tipada a `float64`.

### A9. `VERIFY BACKUP` (y `BACKUP`/`RESTORE` cifrados incrementales) rechazaban a la
consola local aunque fuera `ADMIN_GENERAL`

`cmd_backup_jsonl.go` exigía `IsAdminRole(sess.authRole)` a secas. En el resto del motor
la consola local sin autenticar tiene `sess.authRole == ""`, y esa cadena vacía se trata
en todas partes como acceso total (superusuario local) — pero `IsAdminRole("")` da
`false`, porque `""` no es literalmente `ADMIN` ni `ADMIN_GENERAL`. Resultado:
`VERIFY BACKUP zoo` fallaba con *"solo un administrador ... puede ejecutar VERIFY
BACKUP"* incluso corriendo como el usuario `ADMIN_GENERAL` por defecto. Arreglado para
seguir el mismo patrón que el resto del código (`sess.authRole != "" && !IsAdminRole(...)`).

### A10. `INSERT` en formato `key=value` / `key:value` guardaba `true`/`false` como texto

`has_water = true` y `active: true` quedaban guardados como el **string** `"true"`, no
como booleano — visible en cualquier `JOIN`/`EXPORT` posterior de esos campos
(`"has_water": "true"` en vez de `"has_water": true`). El valor sin comillas no
coincidía con el parseo numérico ni con el de objeto JSON, y caía en
`strings.Trim(valStr, "\"'")`, que no hace nada si no hay comillas que quitar. El
formato JSON (`{"has_water": true}`) nunca tuvo este problema, porque `encoding/json` ya
resuelve `true`/`false` correctamente. Nuevo helper `parseKVLiteral` usado en ambas
ramas (`=` y `:`), que reconoce `true`/`false`/`null` además de números y objetos.

### A11. `SEARCH ... WITH SCORE` mostraba siempre `Score: 0.00`

El texto de búsqueda llega en `tokens[2]` con las comillas literales todavía pegadas
(el tokenizer las conserva para permitir frases con espacios — ver A7). El bloque que
imprime el score volvía a calcular la relevancia con ese mismo texto **con comillas**,
así que `strings.Contains` nunca encontraba coincidencia contra el valor real de ningún
campo y el score daba `0.00` sin importar cuán bueno fuera el resultado. Arreglado
quitando las comillas (`unquoteSearchTerm`) antes de recalcular el score.

### A12. `SEARCH ... FUZZY` / `SIMILAR` no hacía nada

Dos problemas encadenados:

1. El modo fuzzy solo anteponía un `~` al texto — pero como ese texto todavía traía
   comillas pegadas (ver A11), terminaba anteponiendo el `~` *antes* de la comilla
   (`~"elefant"` en vez de `"~elefant"`), rompiendo incluso esa marca.
2. Aunque el `~` hubiera quedado bien puesto, **nada en el motor lo interpretaba**: el
   despachador de `Search()` solo distinguía frase exacta (comillas) y búsqueda por
   operadores (`+`/`-`); un texto con `~` caía en la búsqueda por relevancia normal,
   que hace coincidencia literal de substring — así que una búsqueda fuzzy de una
   palabra mal escrita nunca encontraba el original bien escrito que se supone debía
   hallar.

Arreglado: `Search()` ahora reconoce el prefijo `~` (ya sin comillas) y lo despacha a
un nuevo `searchFuzzy`, que compara cada palabra buscada contra las palabras de cada
documento por distancia de Levenshtein (tolerancia proporcional al largo de la
palabra), puntuando más los documentos con coincidencias más cercanas. El mismo arreglo
de comillas se aplicó a `EXPLAIN SEARCH`, que duplicaba la misma lógica.

---

## B. Errores del `zoo.nql`

| § | Qué estaba mal | Corrección |
|---|---|---|
| 4.3 | `INSERT species sp_elephant {...}; sp_giraffe {...}` — la forma `;` no admite un id por documento; el 2.º y 3.º caían al parser `key:value`, se partían por las comas internas del JSON e insertaban **documentos basura en silencio** | Array JSON con `_id`, que sí admite id por documento. La forma `;` se sigue ejercitando en `donations`, donde los ids son autogenerados |
| 4.x | Todos los leones referenciaban `species_id: "sp001"`, una especie que nunca se creó (la primera se insertó sin id) | Se crea `sp_lion` con id explícito; la demo de id autogenerado usa una especie que nadie referencia |
| 4.8 | `INSERT animals FROM "animals_seed.json"` / `.csv` — archivos inexistentes | Convertido en ejemplo comentado; el `IMPORT` real se ejercita en §18 con los archivos que el propio script exporta |
| — | El bloque `treatments` se indexaba, relacionaba y consultaba (§29) pero nunca se llenaba | Se añaden tres `INSERT treatments` |
| 10 | `ADD FIELD animals.last_checkup DEFAULT null, animals.insurance_covered DEFAULT false` — solo el primer campo lleva prefijo `<block>.`; el segundo creaba un campo llamado `animals.insurance_covered` (por eso el export muestra `"animals":{"insurance_covered":false}` anidado, y el `DROP FIELD` del segundo campo nunca borró nada) | Campos siguientes escritos sin prefijo |
| 13 | El `UPDATE habitats` estaba dentro de `BEGIN zoo animals` (bloque distinto), y el ejemplo de `ROLLBACK` no probaba nada | Dos transacciones, una por bloque, más un `GET animals bad001` que demuestra que el rollback descartó el documento |
| 14 | `IMPORT ... FROM FILE '/data/animals_bulk.ndjson'` — rutas inventadas | Comentado; `BULK MODE ON/STATUS/OFF` se siguen ejecutando |
| 21 | `CREATE USER ... ROLE USER` sin `DATABASE` — todo rol salvo `ADMIN_GENERAL` pertenece a una base, así que fallaba. Las cláusulas en cascada tampoco se unían | Se añade `DATABASE zoo`; las líneas en cascada terminan en `\` |
| 22 | `GRANT read, write ON ...` — con el espacio, `ON` deja de ser el token siguiente y el comando falla | `GRANT read,write ON ...` |
| 6 | `GET animals @ lion001` | Ya funciona (ver A3) |
| 25 | `BACKUP zoo TO "zoo_2026-09-09.bak"` | Ya funciona (ver A4) |

---

## C. Cómo verificar

```sh
go build ./... && go test ./...
./caimandb run zoo.nql          # ahora ejecutable de principio a fin
```

Señales de que todo fue bien:

- `SHOW BLOCKS zoo` muestra `treatments` con 3 documentos y `species` con 9
  (8 con id explícito + 1 autogenerado), sin documentos basura.
- Tras §13, `GET animals tig002` encuentra el documento y `GET animals bad001` no.
- `backups/animals_export.csv` ya no contiene `map[...]`, `[a b c]` ni `<nil>`,
  y ningún documento tiene un campo llamado `animals`.
