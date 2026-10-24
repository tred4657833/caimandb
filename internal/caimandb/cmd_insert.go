package caimandb

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// parseKVLiteral turns the raw right-hand-side text of a "key=value" or
// "key:value" INSERT field into a typed value: a JSON object stays an
// object, a number becomes a float64, the bare words true/false/null
// become their Go equivalents, and everything else is treated as a
// (possibly quoted) string.
//
// Before this, an unquoted "true"/"false" fell straight through to
// strings.Trim(valStr, `"'`) -- which only strips quote characters, so
// on a value with no quotes to strip it just returns the word
// "true"/"false" unchanged, but as a *string*, not a boolean. That's
// how key=value/key:value INSERTs like "has_water = true" or
// "active: true" ended up storing the literal text "true" instead of
// the boolean true -- visible later in any JOIN/EXPORT of that field.
// JSON-document INSERTs (the {"has_water": true} form) never had this
// problem, since encoding/json already resolves true/false correctly.
func parseKVLiteral(valStr string) any {
	if num, err := strconv.ParseFloat(valStr, 64); err == nil {
		return num
	}
	if strings.HasPrefix(valStr, "{") {
		var nested map[string]any
		if err := json.Unmarshal([]byte(valStr), &nested); err == nil {
			return nested
		}
		return strings.Trim(valStr, "\"'")
	}
	switch strings.ToLower(valStr) {
	case "true":
		return true
	case "false":
		return false
	case "null":
		return nil
	}
	return strings.Trim(valStr, "\"'")
}

func handleInsert(eng *Engine, sess *Session, tokens []string) string {
	if len(tokens) < 3 {
		return "ERROR: INSERT requires block name and document data"
	}

	block := tokens[1]
	db := sess.currentDB
	if db == "" {
		return "ERROR: No database in use"
	}
	if err := eng.authorizeBlockAccess(sess, db, block, "write"); err != nil {
		return "ERROR: " + err.Error()
	}

	// INSERT <block> GENERATE <n> -- genera n documentos sintéticos e
	// los inserta por el mismo camino rápido (InsertBatch en lotes) que
	// INSERT FROM/[...]/;-separado, reportando cuánto tardó y a qué
	// ritmo. Pensado para poder medir "¿cuánto tarda insertar 1,000,000
	// de documentos en esta máquina, con este disco?" en vez de
	// adivinarlo.
	if len(tokens) >= 3 && strings.EqualFold(tokens[2], "GENERATE") {
		if len(tokens) < 4 {
			return "ERROR: INSERT <block> GENERATE requires a count, e.g. INSERT users GENERATE 1000000"
		}
		n, err := strconv.Atoi(tokens[3])
		if err != nil || n <= 0 {
			return "ERROR: invalid GENERATE count: " + tokens[3]
		}

		// Optional trailing "WORKERS <w>": how many goroutines split the
		// n documents between them (see handleInsertGenerate). 0 means
		// "let the engine pick a sane default from GOMAXPROCS".
		workers := 0
		switch {
		case len(tokens) >= 6 && strings.EqualFold(tokens[4], "WORKERS"):
			w, err := strconv.Atoi(tokens[5])
			if err != nil || w <= 0 {
				return "ERROR: invalid WORKERS count: " + tokens[5]
			}
			workers = w
		case len(tokens) == 5:
			return "ERROR: unexpected token after GENERATE <n>: " + tokens[4] + " (did you mean WORKERS <n>?)"
		}

		return handleInsertGenerate(eng, db, block, n, workers, workers > 0)
	}

	var customID string
	var dataTokens []string
	startIdx := 2

	if len(tokens) >= 4 {
		secondToken := tokens[2]
		if !strings.HasPrefix(secondToken, "{") &&
			!strings.HasPrefix(secondToken, "[") &&
			!strings.HasPrefix(secondToken, "\"") &&
			secondToken != "NULL" &&
			!strings.EqualFold(secondToken, "FROM") &&
			!strings.EqualFold(secondToken, "GENERATE") &&
			!strings.EqualFold(secondToken, "TO") &&
			!strings.EqualFold(secondToken, "WHERE") &&
			!strings.EqualFold(secondToken, "SET") &&
			!strings.EqualFold(secondToken, "LIMIT") &&
			!strings.EqualFold(secondToken, "ORDER") &&
			!strings.EqualFold(secondToken, "SELECT") {
			customID = strings.Trim(secondToken, "\"'")
			startIdx = 3
			dataTokens = tokens[startIdx:]
		} else {
			dataTokens = tokens[startIdx:]
		}
	} else {
		dataTokens = tokens[startIdx:]
	}

	if len(dataTokens) == 0 {
		return "ERROR: No document data provided"
	}

	if len(tokens) >= 3 && strings.ToUpper(tokens[2]) == "FROM" {
		if len(tokens) < 4 {
			return "ERROR: INSERT FROM requires file name"
		}
		srcFile := strings.Trim(tokens[3], "\"'")
		inserted, errors, err := eng.Import(db, block, srcFile)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return fmt.Sprintf("Imported: %d documents, Errors: %d", inserted, errors)
	}

	jsonStr := strings.Join(dataTokens, " ")

	if strings.HasPrefix(jsonStr, "{") {
		var data map[string]any
		if err := json.Unmarshal([]byte(jsonStr), &data); err == nil {
			if customID != "" {
				data["id"] = customID
			}
			// Explicit custom IDs are the less common path; they keep
			// using the direct, unbatched call so the hot path (plain
			// INSERT with a generated ID) is the only one that goes
			// through InsertFast's batching. Inside a BEGIN/COMMIT both
			// become a pending mutation instead -- see cmd_tx_dml.go.
			doc, err := execInsertDoc(eng, sess, db, block, customID, data, true)
			if err != nil {
				return "ERROR: " + err.Error()
			}
			if customID != "" {
				return fmt.Sprintf("Inserted document: %s (ID: %s, shard: %s)", doc.ID, customID, doc.ShardID)
			}
			return fmt.Sprintf("Inserted document: %s (shard: %s)", doc.ID, doc.ShardID)
		}
	}

	if strings.HasPrefix(jsonStr, "[") && strings.HasSuffix(jsonStr, "]") {
		var docs []map[string]any
		if err := json.Unmarshal([]byte(jsonStr), &docs); err == nil {
			if len(docs) > 0 {
				if customID != "" {
					docs[0]["id"] = customID
					doc, err := execInsertDoc(eng, sess, db, block, customID, docs[0], false)
					if err != nil {
						return "ERROR: " + err.Error()
					}
					inserted, errors, err := execInsertBatch(eng, sess, db, block, docs[1:])
					if err != nil {
						return "ERROR: " + err.Error()
					}
					return fmt.Sprintf("Inserted: %d documents, Errors: %d (first ID: %s)", inserted+1, errors, doc.ID)
				}
				inserted, errors, err := execInsertBatch(eng, sess, db, block, docs)
				if err != nil {
					return "ERROR: " + err.Error()
				}
				return fmt.Sprintf("Inserted: %d documents, Errors: %d", inserted, errors)
			}
			return "ERROR: Empty document array"
		}
	}

	if strings.Contains(jsonStr, ";") {
		parts := strings.Split(jsonStr, ";")
		docs := make([]map[string]any, 0, len(parts))
		errors := 0

		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			if strings.HasPrefix(part, "{") {
				var data map[string]any
				if err := json.Unmarshal([]byte(part), &data); err == nil {
					docs = append(docs, data)
				} else {
					errors++
				}
				continue
			}

			data := make(map[string]any)
			fields := strings.Split(part, ",")
			success := false

			for _, field := range fields {
				field = strings.TrimSpace(field)
				if field == "" {
					continue
				}

				if strings.Contains(field, "=") {
					kv := strings.SplitN(field, "=", 2)
					if len(kv) == 2 {
						key := strings.TrimSpace(kv[0])
						valStr := strings.TrimSpace(kv[1])
						var parsed any
						if num, err := strconv.ParseFloat(valStr, 64); err == nil {
							parsed = num
						} else if strings.HasPrefix(valStr, "{") {
							var nested map[string]any
							if err := json.Unmarshal([]byte(valStr), &nested); err == nil {
								parsed = nested
							} else {
								parsed = strings.Trim(valStr, "\"'")
							}
						} else {
							parsed = strings.Trim(valStr, "\"'")
						}
						data[key] = parsed
						success = true
					}
				} else if strings.Contains(field, ":") {
					kv := strings.SplitN(field, ":", 2)
					if len(kv) == 2 {
						key := strings.TrimSpace(kv[0])
						valStr := strings.TrimSpace(kv[1])
						var parsed any
						if num, err := strconv.ParseFloat(valStr, 64); err == nil {
							parsed = num
						} else if strings.HasPrefix(valStr, "{") {
							var nested map[string]any
							if err := json.Unmarshal([]byte(valStr), &nested); err == nil {
								parsed = nested
							} else {
								parsed = strings.Trim(valStr, "\"'")
							}
						} else {
							parsed = strings.Trim(valStr, "\"'")
						}
						data[key] = parsed
						success = true
					}
				}
			}

			if success && len(data) > 0 {
				if customID != "" && len(docs) == 0 {
					data["id"] = customID
				}
				docs = append(docs, data)
			} else {
				errors++
			}
		}

		if len(docs) > 0 {
			if customID != "" && len(docs) > 0 {
				docs[0]["id"] = customID
				doc, err := execInsertDoc(eng, sess, db, block, customID, docs[0], false)
				if err != nil {
					return "ERROR: " + err.Error()
				}
				inserted, batchErrors, err := execInsertBatch(eng, sess, db, block, docs[1:])
				if err != nil {
					return "ERROR: " + err.Error()
				}
				return fmt.Sprintf("Inserted: %d documents, Errors: %d (first ID: %s)", inserted+1, batchErrors+int64(errors), doc.ID)
			}
			inserted, batchErrors, err := execInsertBatch(eng, sess, db, block, docs)
			if err != nil {
				return "ERROR: " + err.Error()
			}
			return fmt.Sprintf("Inserted: %d documents, Errors: %d", inserted, batchErrors+int64(errors))
		}

		if errors > 0 {
			return fmt.Sprintf("ERROR: Failed to parse %d document(s)", errors)
		}
	}

	data := make(map[string]any)
	fields := strings.Split(jsonStr, ",")
	success := false

	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		if strings.Contains(field, "=") {
			kv := strings.SplitN(field, "=", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				valStr := strings.TrimSpace(kv[1])
				data[key] = parseKVLiteral(valStr)
				success = true
			}
		} else if strings.Contains(field, ":") {
			kv := strings.SplitN(field, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				valStr := strings.TrimSpace(kv[1])
				data[key] = parseKVLiteral(valStr)
				success = true
			}
		}
	}

	if success && len(data) > 0 {
		if customID != "" {
			data["id"] = customID
		}
		doc, err := execInsertDoc(eng, sess, db, block, customID, data, false)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		if customID != "" {
			return fmt.Sprintf("Inserted document: %s (ID: %s, shard: %s)", doc.ID, customID, doc.ShardID)
		}
		return fmt.Sprintf("Inserted document: %s (shard: %s)", doc.ID, doc.ShardID)
	}

	return "ERROR: Could not parse document data. Use JSON, key:value, or key=value format"
}

// generateBatchSize es cuántos documentos sintéticos se acumulan antes de
// mandarlos juntos a InsertBatch. Subido de 5000 a 20000: para 1M de
// documentos eso baja de 200 a 50 las llamadas a InsertBatch -- cada una
// paga su propia CreateBlockDirectories + lock del bloque + un WriteBatch
// de WAL, así que menos llamadas de nivel superior significa menos de ese
// costo fijo repetido (Badger sigue flusheando cada 1000 dentro de cada
// llamada, así que esto no cambia cuánta memoria usa un solo WriteBatch).
//
// Esto sigue siendo el tamaño de lote para el modo de UN solo worker
// (n pequeño, o WORKERS 1 explícito). El modo multi-worker de abajo usa
// generateWorkerBatchSize, más chico, porque ahí varios workers tienen
// lotes en preparación al mismo tiempo -- generateBatchSize*workers de
// una sola vez sería demasiada memoria en vuelo.
const generateBatchSize = 20000

// generateWorkerBatchSize es el tamaño de lote por worker cuando GENERATE
// corre con varios workers concurrentes (ver handleInsertGenerate). Cada
// worker prepara (JSON + compresión) su propio lote sin ningún lock --
// eso ya corre en paralelo dentro de insertBatchLocalDetailed también,
// así que este número no busca maximizar paralelismo de CPU por sí solo,
// sino mantener el pipeline lleno: mientras el lock del bloque está
// ocupado comiteando el lote de un worker, los demás ya pueden estar
// preparando el suyo. 5000 documentos por lote es chico comparado con el
// generateBatchSize de un solo worker, a propósito -- con más workers en
// vuelo, cada uno necesita un lote más chico para que el conjunto siga
// cabiendo cómodo en memoria.
const generateWorkerBatchSize = 5000

// maxGenerateWorkers pone un techo razonable a WORKERS <n> -- pasado
// cierto punto, más goroutines que núcleos reales solo compiten por el
// mismo lock de bloque sin ganar nada (la sección crítica sigue siendo
// serial), así que un typo como "WORKERS 500" no debería crear 500
// goroutines todas esperando el mismo lock.
const maxGenerateWorkers = 64

// autoScaleCheckInterval es cada cuánto el vigilante de ritmo (ver
// runRateWatchdog) vuelve a medir docs/sec durante un GENERATE sin
// WORKERS explícito. Ni tan seguido que la medición sea ruido (un
// InsertBatch todavía en preparación cuenta cero hasta que hace
// flush), ni tan espaciado que tarde en reaccionar a una caída real.
const autoScaleCheckInterval = 2 * time.Second

// autoScaleWarmup es cuánto se espera antes de fijar el "ritmo de
// referencia" (peakRate) contra el que se compara todo lo demás --
// las primeras muestras incluyen el arranque de goroutines, la
// apertura de Badger, etc., y fijarían un techo artificialmente bajo.
const autoScaleWarmup = 3 * autoScaleCheckInterval

// autoScaleDegradeThreshold: si el ritmo actual cae por debajo de este
// porcentaje del mejor ritmo visto hasta ahora, el vigilante suma un
// worker. 0.85 da margen a jitter normal (GC, flush de Badger) sin
// disparar por ruido, pero reacciona antes de que la caída se note en
// el reloj del operador.
const autoScaleDegradeThreshold = 0.85

// autoScaleStep es cuántos workers se suman cada vez que se detecta
// degradación sostenida. De a uno solo sería demasiado lento para
// alcanzar al ritmo objetivo en cargas grandes; de a muchos de golpe
// arriesga sobrepasar núcleos disponibles y empeorar la contención del
// lock de bloque en vez de compensar.
const autoScaleStep = 2

// autoScaleDefaultMaxMultiplier es el default de
// Config.GenerateAutoScaleMaxMultiplier cuando no está seteado (0): el
// vigilante automático no va a proponer más de GOMAXPROCS *
// autoScaleDefaultMaxMultiplier workers por su cuenta.
const autoScaleDefaultMaxMultiplier = 4

// autoScaleCooldownTicks es cuántos chequeos (de autoScaleCheckInterval
// cada uno) hay que esperar después de sumar workers antes de volver a
// evaluar si hace falta sumar más. Sin este enfriamiento, el vigilante
// original podía disparar en CADA tick mientras el ritmo siguiera por
// debajo del umbral -- incluyendo los ticks inmediatamente posteriores a
// haber sumado workers, cuando esos workers todavía ni terminaron su
// primer batch y el ritmo medido sigue reflejando el estado previo a la
// suma. En hardware con pocos núcleos eso genera un ciclo que se
// retroalimenta: sumar workers de más causa más contención del lock por
// bloque -> el ritmo cae aún más -> el vigilante lo interpreta como
// "todavía faltan workers" y suma otra vez, sin nunca darle tiempo a la
// última tanda a hacer efecto. Con este enfriamiento, cada suma de
// workers tiene autoScaleCooldownTicks*autoScaleCheckInterval para
// mostrar si realmente ayudó antes de que se considere sumar más.
const autoScaleCooldownTicks = 2

// maxGenerateCount es un tope de sanidad para evitar que un typo (un
// cero de más) intente generar miles de millones de documentos.
const maxGenerateCount = 50_000_000

// autoBulkModeMinCount es el umbral de "esto ya es una carga masiva" a
// partir del cual GENERATE activa BULK MODE por su cuenta (ver más abajo)
// en lugar de exigirle al operador que se acuerde de escribir
// "BULK MODE ON" antes. Puesto en generateBatchSize: por debajo de un
// lote no hay ganancia real (ya es una sola llamada a InsertBatch); desde
// ahí para arriba, cuantos más lotes, más pesa el fsync por lote que BULK
// MODE evita.
const autoBulkModeMinCount = generateBatchSize

var genFirstNames = []string{"Ana", "Luis", "Marta", "Carlos", "Sofia", "Diego", "Elena", "Mateo", "Valeria", "Hugo"}
var genDomains = []string{"example.com", "mail.com", "test.org", "demo.net"}

// genDoc construye el documento sintético #i -- extraído a su propia
// función para que tanto el camino de un solo worker como el de varios
// (ver defaultGenerateWorkers/handleInsertGenerate) generen exactamente
// el mismo documento a partir del mismo índice, sin duplicar el literal.
func genDoc(runIDPrefix string, i int) map[string]any {
	name := genFirstNames[i%len(genFirstNames)]
	return map[string]any{
		"id":    runIDPrefix + fmt.Sprintf("%08x", i),
		"name":  fmt.Sprintf("%s_%d", name, i),
		"email": fmt.Sprintf("user%d@%s", i, genDomains[i%len(genDomains)]),
		// seq/value/active/created_at van anidados a propósito (no
		// como campos de primer nivel): buildSecondaryIndexInto solo
		// indexa string/número/bool de primer nivel, nunca el
		// contenido de un map -- así que agruparlos acá adentro los
		// saca de la indexación automática en vez de pagar una
		// escritura de índice extra por cada uno. Siguen siendo
		// consultables vía FIND con notación de punto (ej.
		// "extra.active"), solo que por full-scan en vez de índice,
		// que es exactamente lo que uno quiere para un GENERATE de
		// benchmark: medir el costo real de insertar+indexar los
		// campos por los que de verdad se filtra (name/email), no
		// inflarlo indexando todo lo que el documento sintético trae.
		"extra": map[string]any{
			"seq":        i,
			"value":      float64(i%1000) * 1.37,
			"active":     i%3 != 0,
			"created_at": time.Now().Unix(),
		},
	}
}

// defaultGenerateWorkers elige cuántos workers usar cuando GENERATE no
// trae "WORKERS <n>" explícito: uno por núcleo lógico (GOMAXPROCS),
// acotado por maxGenerateWorkers y por no tener más workers que lotes
// de trabajo reales que repartir.
func defaultGenerateWorkers(n int) int {
	if n <= generateWorkerBatchSize {
		// Un solo lote no necesita coordinación entre goroutines --
		// correr con un worker (el camino secuencial de siempre) es
		// más simple y no deja nada sobre la mesa: no hay un segundo
		// lote con el que solaparse.
		return 1
	}
	w := runtime.GOMAXPROCS(0)
	if w > maxGenerateWorkers {
		w = maxGenerateWorkers
	}
	if w < 1 {
		w = 1
	}
	return w
}

// handleInsertGenerate implementa "INSERT <block> GENERATE <n> [WORKERS
// <w>]": genera n documentos sintéticos e los inserta por el camino rápido
// (lotes vía InsertBatch), midiendo el tiempo real -- pensado como comando
// de benchmark para que un operador pueda medir en su propia
// máquina/disco cuánto tarda insertar un volumen dado, en vez de que
// alguien le prometa un número sin haberlo corrido.
//
// workers <= 0 significa "elegí un default razonable" (ver
// defaultGenerateWorkers) Y deja el vigilante de ritmo activado (ver
// runRateWatchdog): el trabajo [0, n) se parte en chunks de
// generateWorkerBatchSize documentos que las goroutines van reclamando de
// una cola atómica (en vez de un tramo fijo asignado de antemano), así que
// sumar un worker a mitad de carrera es tan simple como lanzar una
// goroutine más -- reclama chunks igual que las demás, sin reparto previo
// que reajustar. Si el ritmo (docs/sec) cae por debajo de
// autoScaleDegradeThreshold del mejor ritmo visto, el vigilante suma
// workers hasta compensar o llegar a maxGenerateWorkers -- pensado para
// que "100,000 en 20s" siga siendo cierto en "200,000 en 40s" en vez de
// degradarse a medida que crece el bloque (más índice, más compactación
// de Badger, etc.).
//
// explicitWorkers=true (WORKERS <n> puesto a mano) desactiva el vigilante:
// un operador que fija el número de workers a propósito -- por ejemplo
// para comparar rendimiento entre corridas -- se queda exactamente con
// ese número, sin que algo lo cambie a mitad de carrera.
//
// Cada goroutine arma sus propios lotes de generateWorkerBatchSize
// documentos y los manda a InsertBatch de forma independiente. Como
// insertBatchLocalDetailed (ops_insert.go) ahora solo toma el lock del
// bloque para el commit real -- no para preparar (JSON + compresión) el
// lote -- varias goroutines sí logran solaparse de verdad: mientras una
// está comiteando (I/O), las demás ya están preparando su siguiente lote
// (CPU), en vez de quedarse todas esperando en fila desde el principio de
// cada llamada.
func handleInsertGenerate(eng *Engine, db, block string, n, workers int, explicitWorkers bool) string {
	if n > maxGenerateCount {
		return fmt.Sprintf("ERROR: GENERATE count too large (max %d) -- if you really need more, run it in several batches", maxGenerateCount)
	}
	if workers <= 0 {
		workers = defaultGenerateWorkers(n)
	}
	if workers > maxGenerateWorkers {
		workers = maxGenerateWorkers
	}
	if workers > n {
		workers = n
	}
	autoScale := !explicitWorkers

	// Un GENERATE de este tamaño ES por definición una carga masiva, así
	// que no debería depender de que el operador se acuerde de escribir
	// "BULK MODE ON" antes -- lo activamos solos por la duración de este
	// comando. Si el operador ya lo tenía prendido a mano (por ejemplo
	// para envolver varios GENERATE/INSERT FROM seguidos), lo dejamos
	// exactamente como estaba: no lo apagamos nosotros al salir, para no
	// pisarle el control manual.
	autoBulk := n >= autoBulkModeMinCount && eng.turbo != nil && !eng.turbo.Bulk.Enabled()
	if autoBulk {
		eng.SetBulkMode(true)
		defer eng.SetBulkMode(false)
	}

	start := time.Now()
	var inserted, errCount atomic.Int64

	// GENERATE bloquea al hilo de la consola hasta terminar y, hasta ahora,
	// no imprimía nada hasta el resultado final -- para una corrida de 1M
	// eso significa quedarse a ciegas 100% del tiempo sin saber si sigue
	// avanzando o si se colgó (BULK STATUS no sirve acá: reporta el turbo
	// async batcher, que GENERATE no usa -- llama a InsertBatch directo).
	// showProgress solo se activa para corridas de más de un lote; una de
	// un solo lote ya termina en el orden de los cientos de ms.
	//
	// Se imprime con \r (nunca \n) directo a stdout para que funcione tanto
	// en modo raw de terminal (ver el comentario de Terminal.out en
	// cli/main.go sobre por qué un \n suelto ahí queda "escalonado") como
	// en modo normal -- \r solo vuelve el cursor a columna 0 y se
	// sobrescribe la misma línea, así que no hace falta pasar por la capa
	// de la terminal.
	//
	// Con varios workers, el progreso lo imprime UN solo goroutine
	// dedicado (por ticker), leyendo los contadores atómicos compartidos
	// -- si cada worker imprimiera el suyo, las líneas \r de goroutines
	// distintas se pisarían entre sí en la terminal.
	showProgress := n > generateWorkerBatchSize
	var progressWG sync.WaitGroup
	stopProgress := make(chan struct{})

	// Un solo crypto/rand.Read para toda la corrida (vía newID()), en vez
	// de uno por documento -- el prefijo aleatorio ya garantiza que esta
	// corrida no choca con datos de una corrida anterior, y el contador
	// hex garantiza unicidad dentro de la corrida, así que no hace falta
	// pagar 1M llamadas a crypto/rand para lo mismo. Un solo prefijo
	// compartido entre todos los workers + el índice global i (único por
	// worker, porque cada uno recorre su propio tramo de [0, n)) sigue
	// garantizando IDs únicos en toda la corrida, igual que en el camino
	// de un solo worker.
	runIDPrefix := newID()

	// totalChunks parte [0, n) en tramos de generateWorkerBatchSize
	// documentos que las goroutines reclaman de a uno por vez de una cola
	// atómica (nextChunk), en vez de un reparto fijo por worker decidido
	// de antemano. Esto es lo que permite que runRateWatchdog sume
	// workers a mitad de carrera sin tener que recalcular ni robarle
	// trabajo a nadie: un worker nuevo simplemente empieza a reclamar
	// chunks igual que los demás.
	totalChunks := (n + generateWorkerBatchSize - 1) / generateWorkerBatchSize
	var nextChunk atomic.Int64
	claimChunk := func() (lo, hi int, ok bool) {
		idx := nextChunk.Add(1) - 1
		if idx >= int64(totalChunks) {
			return 0, 0, false
		}
		lo = int(idx) * generateWorkerBatchSize
		hi = lo + generateWorkerBatchSize
		if hi > n {
			hi = n
		}
		return lo, hi, true
	}

	// El pool de workers es elástico -- runRateWatchdog puede sumar
	// goroutines a mitad de carrera -- así que NO se usa sync.WaitGroup
	// para esperar el final: los docs de sync.WaitGroup advierten
	// explícitamente que un Add(positivo) concurrente con un Wait() ya en
	// curso, si el contador llega a pasar por cero en el medio, es un
	// misuse (panic "WaitGroup misuse: Add called concurrently with
	// Wait"). Acá eso puede pasar de verdad: el vigilante decide sumar un
	// worker justo cuando los que quedaban están terminando. En cambio,
	// se lleva un contador de workers vivos protegido por poolMu, y quien
	// lo lleva a 0 cierra allWorkersDone -- spawnWorker y la salida de
	// cada worker están serializados por el mismo mutex, así que no hay
	// ventana para la carrera.
	var poolMu sync.Mutex
	liveWorkers := 0
	launchedWorkers := 0
	poolFinished := false
	allWorkersDone := make(chan struct{})

	runWorker := func() {
		batch := make([]map[string]any, 0, generateWorkerBatchSize)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			ins, errs := eng.InsertBatchViaLineDat(db, block, batch)
			inserted.Add(ins)
			errCount.Add(errs)
			batch = batch[:0]
		}
		for {
			lo, hi, ok := claimChunk()
			if !ok {
				break
			}
			for i := lo; i < hi; i++ {
				batch = append(batch, genDoc(runIDPrefix, i))
			}
			flush()
		}

		poolMu.Lock()
		liveWorkers--
		if liveWorkers == 0 {
			poolFinished = true
			close(allWorkersDone)
		}
		poolMu.Unlock()
	}
	// spawnWorker es a prueba de llamadas concurrentes (main y el
	// vigilante) y de llamadas después de que el pool ya cerró
	// allWorkersDone -- en ese caso no hace nada, en vez de arriesgar un
	// close(allWorkersDone) duplicado.
	spawnWorker := func() {
		poolMu.Lock()
		if poolFinished {
			poolMu.Unlock()
			return
		}
		liveWorkers++
		launchedWorkers++
		poolMu.Unlock()
		go runWorker()
	}
	activeWorkerCount := func() int {
		poolMu.Lock()
		defer poolMu.Unlock()
		return liveWorkers
	}
	for w := 0; w < workers; w++ {
		spawnWorker()
	}

	// El goroutine de progreso se lanza recién acá -- después de que
	// activeWorkerCount ya existe -- para que printProgress lo capture
	// directamente sin necesitar una variable reasignada más tarde
	// desde otra goroutine (eso sí sería una carrera de datos real,
	// aunque en la práctica improbable de observar por la ventana de
	// tiempo involucrada).
	printProgress := func() {
		ins := inserted.Load()
		elapsed := time.Since(start)
		rate := float64(ins)
		if elapsed.Seconds() > 0 {
			rate = float64(ins) / elapsed.Seconds()
		}
		fmt.Fprintf(os.Stdout, "\rGenerating... %d/%d (%.0f docs/sec, %s elapsed, %d workers)\033[K",
			ins, n, rate, elapsed.Round(time.Second), activeWorkerCount())
	}
	if showProgress {
		progressWG.Add(1)
		go func() {
			defer progressWG.Done()
			ticker := time.NewTicker(400 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					printProgress()
				case <-stopProgress:
					return
				}
			}
		}()
	}

	// runRateWatchdog mide docs/sec cada autoScaleCheckInterval y, si cae
	// por debajo de autoScaleDegradeThreshold del mejor ritmo visto hasta
	// ahora (peakRate), suma autoScaleStep workers -- hasta
	// maxGenerateWorkers o hasta que no queden chunks por repartir. No
	// baja workers si el ritmo mejora: quitar goroutines a mitad de una
	// corrida no vale la pena (I/O en vuelo, riesgo de sub-utilizar si la
	// caída era pasajera), así que solo compensa hacia arriba.
	watchdogDone := make(chan struct{})
	if autoScale && n > generateWorkerBatchSize {
		autoScaleMax := autoScaleMaxWorkers(eng.config.GenerateAutoScaleMaxMultiplier)
		go runRateWatchdog(watchdogDone, &inserted, start, spawnWorker, activeWorkerCount, &nextChunk, totalChunks, autoScaleMax)
	} else {
		close(watchdogDone)
	}

	<-allWorkersDone
	close(watchdogDone)

	if showProgress {
		close(stopProgress)
		progressWG.Wait()
		// Limpia la línea de progreso antes de que el resultado final
		// se imprima (por la capa de arriba, vía Terminal.out) para que
		// no quede pegado al final de "Generating... 999999/1000000...".
		fmt.Fprint(os.Stdout, "\r\033[K")
	}

	elapsed := time.Since(start)
	rate := float64(inserted.Load())
	if elapsed.Seconds() > 0 {
		rate = float64(inserted.Load()) / elapsed.Seconds()
	}
	poolMu.Lock()
	finalWorkers := launchedWorkers
	poolMu.Unlock()
	bulkNote := ""
	if autoBulk {
		bulkNote = " [bulk mode auto-enabled]"
	}
	scaleNote := ""
	if autoScale && finalWorkers > workers {
		scaleNote = fmt.Sprintf(" [auto-scaled %d -> %d workers]", workers, finalWorkers)
	}
	return fmt.Sprintf("Generated & inserted: %d documents into %s.%s in %s (%.0f docs/sec, %d workers), Errors: %d%s%s",
		inserted.Load(), db, block, formatDuration(elapsed), rate, finalWorkers, errCount.Load(), bulkNote, scaleNote)
}

// autoScaleMaxWorkers calcula el techo que el vigilante automático puede
// alcanzar por su cuenta: GOMAXPROCS * multiplier, acotado por
// maxGenerateWorkers (el techo absoluto, el mismo que aplica a un WORKERS
// explícito) y con un piso de autoScaleStep+1 para no quedar por debajo
// del punto de partida en máquinas de un solo núcleo. multiplier<=0 usa
// autoScaleDefaultMaxMultiplier.
//
// Por qué existe: maxGenerateWorkers (64) es un tope de sanidad pensado
// para no crear cientos de goroutines por un typo, no un techo pensado
// para el hardware real de la corrida. En una máquina de 2 núcleos, dejar
// que el vigilante escale automáticamente hasta 64 workers significa
// pasar muy pronto el punto en el que sumar workers deja de ayudar y
// empieza a empeorar las cosas (más cambios de contexto, más contención
// sobre el lock único por bloque en insertBatchLocalDetailed) -- lo cual
// el propio vigilante interpretaba como "más degradación, sumo más
// workers", un ciclo que solo se frenaba al tope absoluto. Atar el techo
// automático al núcleo real de la máquina corta ese ciclo mucho antes.
func autoScaleMaxWorkers(multiplier int) int {
	if multiplier <= 0 {
		multiplier = autoScaleDefaultMaxMultiplier
	}
	max := runtime.GOMAXPROCS(0) * multiplier
	if max < autoScaleStep+1 {
		max = autoScaleStep + 1
	}
	if max > maxGenerateWorkers {
		max = maxGenerateWorkers
	}
	return max
}

// runRateWatchdog vive mientras dure el GENERATE (hasta que done se
// cierra) muestreando inserted.Load() cada autoScaleCheckInterval. Después
// de autoScaleWarmup fija peakRate al mejor ritmo instantáneo visto; de
// ahí en más, cualquier muestra por debajo de peakRate*autoScaleDegradeThreshold
// dispara spawnWorker() autoScaleStep veces (acotado por autoScaleMax --
// ver autoScaleMaxWorkers -- y por chunks restantes: no tiene sentido
// sumar workers si ya no queda trabajo por repartir).
//
// Enfriamiento: después de sumar workers, el vigilante se queda
// autoScaleCooldownTicks chequeos sin volver a evaluar degradación. Esto
// es lo que evita que una tanda de sumas en cadena (cada tick viendo
// todavía el ritmo bajo porque los workers nuevos ni arrancaron su primer
// batch) se dispare varias veces seguidas por la misma causa una vez
// resuelta. Durante el enfriamiento el ritmo sigue actualizando peakRate
// si mejora, para no perder una mejora real que ocurra en esa ventana.
func runRateWatchdog(done <-chan struct{}, inserted *atomic.Int64, start time.Time, spawnWorker func(), activeWorkerCount func() int, nextChunk *atomic.Int64, totalChunks int, autoScaleMax int) {
	ticker := time.NewTicker(autoScaleCheckInterval)
	defer ticker.Stop()

	var lastCount int64
	lastAt := start
	var peakRate float64
	warmedUp := false
	cooldownRemaining := 0

	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			cur := inserted.Load()
			dt := now.Sub(lastAt).Seconds()
			if dt <= 0 {
				continue
			}
			instRate := float64(cur-lastCount) / dt
			lastCount = cur
			lastAt = now

			if !warmedUp {
				if now.Sub(start) < autoScaleWarmup {
					continue
				}
				warmedUp = true
				peakRate = instRate
				continue
			}
			if instRate > peakRate {
				peakRate = instRate
			}
			if cooldownRemaining > 0 {
				cooldownRemaining--
				continue
			}
			if instRate > peakRate || peakRate <= 0 || instRate >= peakRate*autoScaleDegradeThreshold {
				continue
			}
			// Ritmo degradado: no sumar workers si ya no queda trabajo
			// por reclamar (la cola se está vaciando, no compensaría
			// nada) o si ya se llegó al techo automático para este
			// hardware.
			if nextChunk.Load() >= int64(totalChunks) {
				continue
			}
			room := autoScaleMax - activeWorkerCount()
			if room <= 0 {
				continue
			}
			step := autoScaleStep
			if step > room {
				step = room
			}
			for i := 0; i < step; i++ {
				spawnWorker()
			}
			cooldownRemaining = autoScaleCooldownTicks
		}
	}
}
