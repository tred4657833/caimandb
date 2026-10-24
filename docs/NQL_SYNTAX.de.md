# CaimanDB NQL — Vollständige Syntaxreferenz (Deutsch)

Andere Sprachen: [English](./NQL_SYNTAX.en.md) · [Español](./NQL_SYNTAX.es.md)

Dieses Dokument deckt die von CaimanDB implementierte NQL-Befehlsschnittstelle ab, einschließlich des vollständigen Befehlsindex, der Syntaxvarianten und Beispiele.

**Verwendete Konventionen:**
- `<block>` — ein Blockname, optional als `<db>.<block>` für einen datenbankübergreifenden Verweis.
- `<db>` — ein Datenbankname.
- `[...]` — optionaler Teil. `<a|b>` — eines auswählen. `...` — wiederholbar.
- Anweisungs-Tokens unterscheiden nicht zwischen Groß-/Kleinschreibung
  (`insert`, `INSERT`, `Insert` funktionieren alle); diese Referenz
  verwendet konventionell GROSSBUCHSTABEN für Schlüsselwörter.

## Inhaltsverzeichnis
- [Datenbankbefehle](#datenbankbefehle)
- [Blockbefehle](#blockbefehle)
- [Benannte Indizes](#benannte-indizes)
- [INSERT — alle Varianten](#insert--alle-varianten)
- [FIND / GET — Abfragen](#find--get--abfragen)
- [SEARCH — Volltextsuche](#search--volltextsuche)
- [UPDATE](#update)
- [DELETE](#delete)
- [ADD FIELD / DROP FIELD / TRUNCATE / UPSERT](#add-field--drop-field--truncate--upsert)
- [Aggregationen (COUNT/SUM/AVG/...)](#aggregationen-countsumavg)
- [GROUP BY](#group-by)
- [ACID-Transaktionen](#acid-transaktionen)
- [TURBO / Massenladen (BULK)](#turbo--massenladen-bulk)
- [JOIN](#join)
- [RELATE](#relate)
- [AUTORELATIONS](#autorelations)
- [Ansichten (VIEWS)](#ansichten-views)
- [EXPORT / IMPORT](#export--import)
- [RENAME FIELD — Feld in allen Dokumenten umbenennen](#rename-field--feld-in-allen-dokumenten-umbenennen)
- [FLEX-COLUMN](#flex-column)
- [Benutzerverwaltung](#benutzerverwaltung)
- [Zugriffskontrolle (GRANT / REVOKE / SHOW GRANTS)](#zugriffskontrolle-grant--revoke--show-grants)
- [Shard-Verwaltung](#shard-verwaltung)
- [Cluster](#cluster)
- [CHECKPOINT](#checkpoint)
- [Navigation & System](#navigation--system)
- [Filteroperatoren](#filteroperatoren)
- [Vollständiges Beispiel](#vollständiges-beispiel)

---

### Datenbankbefehle

```
CREATE DB <name>                    Neue Datenbank erstellen
DROP DB <name>                      Datenbank löschen
RENAME DB <alt> TO <neu>            Datenbank umbenennen
USE <name>                          Zu dieser Datenbank wechseln (aktuelle Sitzung)
SHOW DBS                            Alle Datenbanken auflisten (Blöcke/Docs/Größe)
SHOW DBS <name> [<name2> ...]       Nur die genannten Datenbanken auflisten
INFO DB <name>                      Datenbankdetails anzeigen
DESCRIBE DB <name>                  Schema anzeigen (abgeleitete Feldtypen)
STATS DB [<name>]                   Datenbankstatistiken anzeigen
SIZE DB [<name>]                    Größe auf der Festplatte anzeigen
COMPACT <db>                        Garbage Collection ausführen / Speicher freigeben
ANALYZE DB [<name>]                 Datenbankleistung analysieren
OPTIMIZE DB [<name>]                Datenbank optimieren (Indizes, Storage-Tiers)
BACKUP <db> TO <datei>              Datenbank in eine Datei sichern
RESTORE <db> FROM <datei>           Datenbank aus einer Sicherung wiederherstellen
```

```sql
CREATE DB shop
USE shop
SHOW DBS
SHOW DBS shop analytics
INFO DB shop
STATS DB shop
BACKUP shop TO "shop_2026-08.bak"
RESTORE shop FROM "shop_2026-08.bak"
DROP DB alter_shop
```

### Blockbefehle

Ein Block ist CaimanDBs Entsprechung einer Tabelle/Collection — ein
benannter, schemafreier Container für Dokumente innerhalb einer Datenbank.

```
CREATE BLOCK [<db>] <name>          Neuen Block erstellen
CREATE BLOCK [<db>] (<n1>, <n2>, ...) Mehrere Blöcke in einer Anweisung erstellen
DROP BLOCK [<db>] <name>            Block löschen
RENAME BLOCK [<db>] <alt> TO <neu>  Block umbenennen
RENAME BLOCK [<db>] (<a1>,<a2>,...) TO (<n1>,<n2>,...)
                                     Mehrere Blöcke parallel, positionsweise umbenennen
SHOW BLOCKS [<db>]                  Alle Blöcke auflisten (Docs/Größe/Shards)
SHOW BLOCKS <db> <name> [<name2>]   Nur die genannten Blöcke auflisten
INFO BLOCK [<db>] <name>            Blockdetails anzeigen
DESCRIBE BLOCK [<db>] <name>        Blockschema anzeigen
EMPTY BLOCK [<db>] <name>           Alle Dokumente aus dem Block löschen
CLEAR [<db>] <name>                 Alias für EMPTY BLOCK
ANALYZE BLOCK [<db>] <name>         Blockleistung analysieren
OPTIMIZE BLOCK [<db>] <name>        Block optimieren
REBUILD BLOCK [<db>] <name>         Alle Indizes neu aufbauen
CHECK BLOCK [<db>] <name>           Blockintegrität prüfen
REPAIR BLOCK [<db>] <name>          Beschädigten Block reparieren
SIZE BLOCK [<db>] <name>            Größe des Blocks auf der Festplatte anzeigen
```

```sql
CREATE BLOCK produkte
CREATE BLOCK shop produkte          -- explizite db, ohne USE
CREATE BLOCK (produkte, kunden, bestellungen)          -- 3 Blöcke, aktuelle db (USE)
CREATE BLOCK shop (produkte, kunden, bestellungen)     -- 3 Blöcke, explizite db
SHOW BLOCKS
SHOW BLOCKS shop produkte inventar
DESCRIBE BLOCK produkte
REBUILD BLOCK produkte              -- z.B. nach Änderung indizierter Felder
EMPTY BLOCK produkte                 -- behält den Block, löscht seine Dokumente
CLEAR produkte                       -- dasselbe wie oben
RENAME BLOCK produkt TO produkt_neu
RENAME BLOCK (produkt, kunde, regal) TO (produkte, kunden, regale)
```

**Strukturhinweise — `CREATE BLOCK` / `RENAME BLOCK` mit Klammerliste:**
- Die Liste `(<a>, <b>, ...)` wird durch Zählen balancierter Klammern
  geparst (`parseParenList`), nicht anhand fester Token-Positionen: sie
  kann Leerzeichen enthalten, und Namen dürfen in Anführungszeichen
  stehen (diese werden automatisch entfernt).
- Jeder Block wird in einem **unabhängigen** Aufruf erstellt/umbenannt
  — das ist keine atomare Transaktion. Schlägt einer fehl (z.B.
  existiert er schon, oder der alte Name existiert nicht), zeigt das
  Ergebnis genau, welche erfolgreich waren und welche nicht; der Rest
  der Liste wird trotzdem verarbeitet.
- Bei `RENAME BLOCK (...) TO (...)` müssen die Liste der alten und die
  der neuen Namen **gleich lang** sein; die Umbenennung erfolgt
  positionsweise (`alt[i] -> neu[i]`). Eine Längenabweichung liefert
  einen Fehler, bevor irgendetwas umbenannt wird.
- Ein Block hat keine festen Spalten/Typen, die bei der Erstellung
  deklariert werden (CaimanDB ist eine Dokumenten-Engine, keine
  relationale Datenbank): die "Struktur" eines Blocks ergibt sich aus
  seinen Dokumenten und kann später mit `DESCRIBE BLOCK` (abgeleitete
  Feldtypen) untersucht werden — es gibt keine Klausel
  `CREATE BLOCK ... (feld TYP, ...)`.

### Benannte Indizes

```
CREATE INDEX <name> ON <block> (<feld>)
CREATE UNIQUE INDEX <name> ON <block> (<feld>)
CREATE INDEX <name> ON <block> (<feld>) WHERE <bedingung>
DROP INDEX <name> ON <block>
SHOW INDEXES ON <block>
```

```sql
CREATE INDEX idx_price ON products (price)
CREATE UNIQUE INDEX idx_sku ON products (sku)
CREATE INDEX idx_active ON products (status) WHERE status = "active"
SHOW INDEXES ON products
DROP INDEX idx_price ON products
```

- `UNIQUE` wird bei einem normalen `INSERT` durchgesetzt (lehnt einen
  doppelten Wert ab).
- Die `WHERE`-Klausel wird gespeichert und von `SHOW INDEXES ON`
  angezeigt, aber bei der Abfrageplanung noch nicht durchgesetzt — es
  wird unabhängig von der Bedingung jedes Dokument indiziert (heute ein
  "partieller" Index nur auf dem Papier, in der Praxis ein voller).
- Es wird nur ein einzelnes Feld pro Index unterstützt: `(<feld>)`.
- Das ist etwas anderes als `SHOW INDEXES [<db>]` (§2), das die flache
  Liste indizierter Feldnamen auf Datenbankebene zeigt; `SHOW INDEXES
  ON <block>` listet die benannten `CREATE INDEX`-Definitionen für
  einen einzelnen Block.

### INSERT — alle Varianten

```
INSERT <block> [<id>] <json-objekt>
INSERT <block> [<id>] schluessel: wert, schluessel2: wert2, ...
INSERT <block> [<id>] schluessel = wert, schluessel2 = wert2, ...
INSERT <block> <doc1>; <doc2>; <doc3>; ...
INSERT <block> [<json-array-von-docs>]
INSERT <block> FROM "<datei.json|datei.csv>"
INSERT <block> GENERATE <n> [WORKERS <w>]
```

**Strukturhinweise:**
- `<id>` ist optional und muss, falls angegeben, das Token direkt nach dem
  Blocknamen sein und darf nicht wie `{`, `[`, `"`, `NULL` oder ein
  reserviertes Schlüsselwort aussehen (`FROM`, `GENERATE`, `TO`, `WHERE`,
  `SET`, `LIMIT`, `ORDER`, `SELECT`) — diese werden stattdessen als Beginn
  des Dokuments/der Klausel interpretiert.
- Mit expliziter ID wird das Dokument mit genau dieser `_id` eingefügt
  (über `insertWithID`) statt mit einer automatisch generierten.
- `schluessel: wert` und `schluessel = wert` funktionieren beide für flache
  Dokumente; Werte werden automatisch typisiert: was als Zahl geparst
  werden kann, wird eine Zahl, `{...}` wird ein verschachteltes Objekt,
  alles andere bleibt ein getrimmter String.
- Sowohl mehrere durch `;` getrennte Dokumente als auch ein JSON-Array auf
  oberster Ebene (`[...]`) fügen einen Stapel in einem Aufruf ein; bei
  angegebener eigener ID wird sie nur auf das erste Dokument angewendet,
  der Rest erhält generierte IDs.

**JSON-Dokument:**
```sql
INSERT produkte {"name": "Tastatur", "preis": 49.90, "auf_lager": true}
INSERT produkte {"benutzer": {"name": "Johann", "alter": 30}}
```

**JSON-Dokument mit expliziter ID (das ID-Token steht direkt nach dem Blocknamen):**
```sql
INSERT produkte kb001 {"name": "Tastatur", "preis": 49.90, "auf_lager": true}
-- -> Inserted document: kb001 (ID: kb001, shard: shard_7)
```

**Schlüssel:Wert-Format:**
```sql
INSERT produkte name: "Maus", preis: 19.90, auf_lager: true
INSERT produkte maus001 name: "Maus", preis: 19.90, auf_lager: true
```

**Schlüssel=Wert-Format:**
```sql
INSERT produkte name = "Monitor", preis = 199.00
```

**Mehrere Dokumente (durch Semikolon getrennt), in einer Anweisung:**
```sql
INSERT produkte {"name": "A"}; {"name": "B"}; {"name": "C"}
INSERT produkte name: "A"; name: "B"; name: "C"
```

**Stapel-Einfügung (JSON-Array):**
```sql
INSERT produkte [{"name": "A"}, {"name": "B"}, {"name": "C"}]
```

**Import aus einer Datei (blockierend, liest die gesamte Datei):**
```sql
INSERT produkte FROM "produkte.json"
INSERT produkte FROM "produkte.csv"
```

**GENERATE — synthetische Daten für Benchmarking / Befüllung:**
```sql
INSERT produkte GENERATE 1000000              -- automatisch skalierte Worker
INSERT produkte GENERATE 200000 WORKERS 8     -- feste Worker-Anzahl (bis 64)
```
- Ohne `WORKERS <n>` misst ein interner Watchdog (`runRateWatchdog`) den
  Durchsatz alle 2s und fügt jeweils 2 Worker hinzu, sobald die Rate unter
  85 % der bisher besten Rate fällt, begrenzt auf
  `GOMAXPROCS * GenerateAutoScaleMaxMultiplier` (Standard-Multiplikator: 4)
  und eine absolute Obergrenze von 64 Workern, mit einer Abkühlphase
  zwischen den Erhöhungen.
- Mit explizitem `WORKERS <n>` wird genau diese Worker-Anzahl für den
  gesamten Lauf verwendet (bis zur gleichen absoluten Obergrenze von 64),
  und der Watchdog wird deaktiviert.
- Große `GENERATE`-Läufe schalten für ihre Dauer automatisch in den BULK MODE.

### FIND / GET — Abfragen

```
FIND <block> [SELECT <feld>[,<feld>...] | <feld> AS <alias> | COUNT(<feld>) AS <alias> | <feld>/<n> AS <alias>]
             [WHERE <bedingung>]
             [DISTINCT [<feld>[,<feld>...]]]
             [GROUP BY <feld>[,<feld>...]] [HAVING <bedingung>]
             [ORDER <feld>[:ASC|:DESC][,<feld>...]]
             [LIMIT <n>] [OFFSET <n>]
             [--type:table]

GET <block> <id>
GET <block> @ <id>

EXPLAIN FIND ...     -- führt die Abfrage wirklich aus und berichtet, was geschah
EXPLAIN SEARCH ...   -- (wie EXPLAIN ANALYZE, nicht nur eine Planschätzung)
```

**Einfache Suche / nach ID:**
```sql
FIND produkte WHERE _id = "abc123"
GET produkte abc123
GET produkte @ abc123
```

**Filter (vollständige Operatorentabelle siehe §21):**
```sql
FIND produkte WHERE preis > 20 AND auf_lager = true
FIND produkte WHERE name LIKE "%tatur%" OR name CONTAINS "Mon"
FIND produkte WHERE preis BETWEEN 10 AND 100
FIND produkte WHERE status IN ("aktiv", "ausstehend")
FIND produkte WHERE tags IN ["go", "datenbank", "nosql"]
```

**Gruppierung/Vorrang mit Klammern und NOT (nur FIND/SEARCH):**
```sql
FIND produkte WHERE (status = "aktiv" OR status = "test") AND preis >= 18
FIND produkte WHERE NOT (status = "gesperrt" OR status = "suspendiert")
```

**Projektion (SELECT):**
```sql
FIND produkte SELECT name, preis WHERE preis > 20
```

**Berechnete SELECT-Felder — COUNT(feld), einfache Arithmetik AS Alias:**
```sql
FIND filme SELECT titel, COUNT(schauspieler) as anzahl_schauspieler, jahr
FIND filme SELECT titel, dauer_minuten / 60 as stunden
```

**GROUP BY / HAVING (nur FIND):**
```sql
FIND filme SELECT titel, COUNT(schauspieler) as anzahl_schauspieler, jahr
  WHERE jahr >= 2000
  GROUP BY titel, jahr
  HAVING COUNT(schauspieler) >= 5
```

**DISTINCT — Duplikate entfernen:**
```sql
FIND products DISTINCT                    -- dedupliziert über die gesamte projizierte Zeile
FIND products DISTINCT category           -- dedupliziert über den Wert eines Felds
FIND products DISTINCT category, status   -- dedupliziert über die Kombination der Felder
FIND products SELECT category DISTINCT category WHERE price > 10
```
`FIND <block> DISTINCT` ohne Felder behält pro unterschiedlicher
projizierter Zeile das erste Dokument; `DISTINCT <feld>,...` behält pro
unterschiedlicher Kombination dieser Feldwerte das erste Dokument.

**Filtern über einen RELATE-Alias (siehe §13):**
```sql
RELATE filme USE regisseure
FIND filme SELECT titel, regisseure.name
  WHERE regisseure.name == "Christopher Nolan"
```

**Sortierung, Paginierung, Tabellenausgabe:**
```sql
FIND produkte ORDER name, preis:DESC WHERE preis > 18
FIND produkte WHERE preis > 18 LIMIT 50 OFFSET 100
FIND produkte WHERE preis > 18 --type:table
```

**EXPLAIN:**
```sql
EXPLAIN FIND produkte WHERE preis > 18 ORDER preis:DESC LIMIT 10
EXPLAIN SEARCH produkte "kabellose tastatur"
```

### SEARCH — Volltextsuche

```
SEARCH <block> "<text>" [EXACT | FUZZY]
                         [WITH SCORE] [WITH MATCHES]
                         [WHERE <bedingung>] [LIMIT <n>] [ORDER <feld>]
```

```sql
SEARCH produkte "kabellose tastatur"
SEARCH produkte "exakte phrase" EXACT
SEARCH produkte "~tastaur" FUZZY
SEARCH produkte "tastatur" WITH SCORE WITH MATCHES
SEARCH produkte "+muss_enthalten -darf_nicht_enthalten optional"
SEARCH produkte "tastatur" WHERE preis > 18 LIMIT 50 ORDER name
```

### UPDATE

```
UPDATE <block> WHERE <bedingung> SET <feld> = <wert>[, <feld2> = <wert2> ...]
UPDATE <block> WHERE <bedingung> INC <feld> = <n>
UPDATE <block> WHERE <bedingung> DEC <feld> = <n>
UPDATE <block> WHERE <bedingung> PUSH <feld> = <wert>
UPDATE <block> WHERE <bedingung> PULL <feld> = <wert>
UPDATE ALL <block> SET <feld> = <wert>[, ...]
```

- `SET` ersetzt Feldwerte. `INC`/`DEC` addieren/subtrahieren eine Zahl von
  einem numerischen Feld. `PUSH`/`PULL` fügen einem Array-Feld einen Wert
  hinzu bzw. entfernen ihn.
- `UPDATE ALL` gilt für jedes Dokument im Block, `WHERE` ist nicht nötig.
- Klauseln können mehrere Zuweisungen und Funktionen wie `now()` kombinieren.

```sql
UPDATE produkte WHERE _id = "kb001" SET name = "Mechanische Tastatur", preis = 55
UPDATE produkte WHERE _id = "kb001" INC aufrufe = 1
UPDATE produkte WHERE _id = "kb001" DEC lagerbestand = 5
UPDATE produkte WHERE _id = "kb001" PUSH tags = "im_angebot"
UPDATE produkte WHERE _id = "kb001" PULL tags = "eingestellt"
UPDATE ALL produkte SET status = "archiviert"
UPDATE produkte WHERE status = "entwurf" SET status = "veroeffentlicht", veroeffentlicht_am = now()
```

### DELETE

```
DELETE <block> WHERE <bedingung>
DELETE ALL <block>
EMPTY BLOCK [<db>] <name>     -- Alias für DELETE ALL
CLEAR [<db>] <name>           -- Alias für DELETE ALL / EMPTY BLOCK
```

```sql
DELETE produkte WHERE _id = "kb001"
DELETE produkte WHERE preis < 5 OR auf_lager = false
DELETE ALL produkte
```

### ADD FIELD / DROP FIELD / TRUNCATE / UPSERT

```
ADD FIELD <block>.<feld> DEFAULT <wert>[, <feld2> DEFAULT <wert2> ...]
DROP FIELD <block>.<feld>[, <feld2> ...] [WHERE <bedingung>]
TRUNCATE <block>
UPSERT <block> <id> SET <feld>=<wert>[, <feld2>=<wert2> ...]
UPSERT <block> <id> <json-objekt>
```

```sql
ADD FIELD products.discount DEFAULT 0
ADD FIELD products.discount DEFAULT 0, products.featured DEFAULT false

DROP FIELD products.discount
DROP FIELD products.discount, products.featured
DROP FIELD products.legacy_sku WHERE category = "discontinued"

TRUNCATE products                    -- leert den Block, ohne Bestätigungsdialog

UPSERT products kb001 SET name = "Mechanical Keyboard", price = 55
UPSERT products kb001 {"name": "Mechanical Keyboard", "price": 55}
```

- `ADD FIELD`: Nur das erste Feld trägt das Präfix `<block>.`; jedes
  weitere Feld nach einem Komma wird demselben Block zugerechnet. Ein
  Dokument, das das Feld bereits besitzt (auch mit einem
  falsy/null/leeren Wert), bleibt unverändert — es werden nur
  Dokumente ergänzt, denen das Feld fehlt.
- `DROP FIELD` ohne `WHERE` läuft in einem Durchgang über den gesamten
  Block; mit `WHERE` wertet es den vollständigen Bedingungsbaum aus
  (inklusive ORs/NOTs/Klammern, wie bei `FIND`) und aktualisiert nur
  die passenden Dokumente.
- `TRUNCATE` ist bedingungslos und sofort, funktional äquivalent zu
  `CLEAR`/`EMPTY BLOCK`, aber an der SQL-`TRUNCATE`-Semantik orientiert
  (kein Bestätigungsdialog).
- `UPSERT` aktualisiert das Dokument, falls `<id>` bereits existiert;
  andernfalls wird ein neues Dokument mit dieser id eingefügt. Es
  akzeptiert entweder kommagetrennte `SET feld=wert`-Zuweisungen oder
  einen rohen JSON-Objektkörper.

### Aggregationen (COUNT/SUM/AVG/...)

```
COUNT  <block> [WHERE <bedingung>]
SUM    <block> <feld> [WHERE <bedingung>]
AVG    <block> <feld> [WHERE <bedingung>]
MIN    <block> <feld> [WHERE <bedingung>]
MAX    <block> <feld> [WHERE <bedingung>]
MEDIAN <block> <feld> [WHERE <bedingung>]
MODE   <block> <feld> [WHERE <bedingung>]
STDDEV <block> <feld> [WHERE <bedingung>]
```

```sql
COUNT produkte WHERE auf_lager = true
SUM bestellungen betrag WHERE status = "abgeschlossen"
AVG produkte preis WHERE kategorie = "elektronik"
MIN produkte preis
MAX produkte preis
MEDIAN gehaelter betrag
MODE produkte kategorie
STDDEV punktzahlen wert
```

### GROUP BY

```
GROUP <block> BY <feld> [COUNT | SUM | AVG | MIN | MAX] [<feld>] [WHERE <bedingung>]
```

```sql
GROUP benutzer BY stadt COUNT
GROUP bestellungen BY status SUM betrag
GROUP produkte BY kategorie AVG preis WHERE preis > 10
GROUP logs BY stufe COUNT WHERE timestamp > "2024-01-01"
```

### ACID-Transaktionen

```
BEGIN [<db> <block>]
  <INSERT|UPDATE|DELETE-Anweisungen...>
COMMIT
ROLLBACK | ABORT

TX STATUS       Details der aktuellen Transaktion anzeigen
TX LIST         Aktive Transaktionen auflisten
TX ISOLATION    Isolationsstufe anzeigen
```

Isolationsstufen (konfiguriert, nicht pro Anweisung wählbar):
`read_committed`, `repeatable_read` (Standard), `serializable`.

```sql
BEGIN shop produkte
  INSERT produkte {"name": "Webcam", "preis": 39.90}
  UPDATE produkte WHERE _id = "kb001" SET preis = 45
  DELETE produkte WHERE _id = "alt001"
COMMIT
```
```sql
BEGIN shop produkte
  INSERT produkte {"name": "Schlechte Idee"}
ROLLBACK
```

### TURBO / Massenladen (BULK)

```
BULK MODE ON            Breitere Batch-Fenster, gelockerte WAL-fsync-Richtlinie
BULK MODE OFF           Normales Verhalten mit niedriger Latenz wiederherstellen
BULK STATUS             Turbo-Engine-Statistiken anzeigen (Worker-Pool, Batching)

IMPORT <block> FROM FILE '<pfad>' [FORMAT NDJSON|ARRAY] [BATCH <n>]
```

- Normale `INSERT`s bündeln bereits gleichzeitige Schreibvorgänge auf
  denselben Block automatisch; `BULK MODE` erweitert das für große Ladungen
  noch weiter.
- `IMPORT ... FROM FILE` streamt eine Datei in großen Batches (Standard
  20000 Docs/Batch) in `<block>`, ohne die gesamte Datei im Speicher zu
  halten. `FORMAT NDJSON` (Standard) liest ein JSON-Objekt pro Zeile;
  `FORMAT ARRAY` liest ein einzelnes JSON-Array oberster Ebene. Ein Pfad mit
  `.gz`-Endung wird on-the-fly entpackt. Läuft für die Dauer des Ladevorgangs
  automatisch unter BULK MODE.

```sql
BULK MODE ON
INSERT produkte GENERATE 2000000
BULK MODE OFF

IMPORT produkte FROM FILE '/data/produkte.ndjson' FORMAT NDJSON BATCH 50000
IMPORT produkte FROM FILE '/data/produkte.json.gz' FORMAT ARRAY
BULK STATUS
```

### JOIN

```
JOIN <block1> WITH <block2> ON <block1>.<feld> = <block2>.<feld>
```

```sql
JOIN bestellungen WITH kunden ON bestellungen.kunden_id = kunden._id
JOIN beitraege WITH benutzer ON beitraege.autor_id = benutzer._id
```

### RELATE

Registriert einmalig, wie sich ein Block auf andere Blöcke bezieht
(optional in anderen Datenbanken); `FIND` löst die Beziehung danach
automatisch auf, statt eine `JOIN`-Bedingung in jeder Abfrage zu wiederholen.

```
RELATE <block> USE <ziel1>[,<ziel2>,...]
```

- Jedes Ziel ist entweder ein einfacher Blockname (gleiche Datenbank) oder
  `db.block` (datenbankübergreifend).
- Konvention für den Abgleich: Das Quelldokument muss ein Feld
  `<ziel>_id` (Singular des Zielblocknamens) mit der ID des Zieldokuments
  haben — eine einzelne ID für Eins-zu-eins/Viele-zu-eins, oder ein
  Array von IDs für Eins-zu-viele.
- Nach der Verknüpfung werden Zielfelder per Punktnotation ausgewählt, oder
  der einfache Alias für das gesamte verknüpfte Dokument.

```sql
RELATE filme USE regisseure,schauspieler,genres
RELATE verkaeufe USE crm.kunden,lager.produkte,buchhaltung.rechnungen

FIND filme SELECT titel,regisseure.name,schauspieler.name
FIND verkaeufe SELECT kunden.name,produkte.name,rechnungen.gesamt
```

### AUTORELATIONS

CaimanDB beobachtet seinen eigenen Lesezugriff: Wenn derselbe Benutzer
dasselbe Dokument wiederholt in einem kurzen Zeitfenster liest (Standard:
5 Lesevorgänge in 10 Minuten), erstellt es automatisch eine Selbstbeziehung
zwischen diesem Benutzer und dem Dokument — kein `RELATE` nötig. Jede
Autorelation trägt `access_count`/`last_seen`, einen `relevance`-Wert und
eine kleine `key_metadata`-Stichprobe der Dokumentfelder.

Im Gegensatz zu `RELATE` (explizit, dauerhaft) sind Autorelationen
temporär: Jeder weitere Zugriff verschiebt ihr Ablaufdatum nach vorne
(Standard-TTL: 24h); sobald ein Dokument von diesem Benutzer nicht mehr
gelesen wird, läuft die Beziehung ab und wird durch einen
Hintergrundprozess entfernt. Der entstehende Graph ist bipartit und
gerichtet (`Benutzer -> gelesenes Dokument`).

```
SHOW AUTORELATIONS <block>
    [FROM <id>] [TO <id>] [DEPTH <n>] [DIRECTION IN|OUT|BOTH]
    [FORMAT TABLE|TREE|GRAPH|JSON]
    [WHERE|FILTER <ausdruck>]
    [ORDER BY DEGREE|ID|NAME|ACCESS_COUNT|RELEVANCE|LAST_SEEN|FIRST_SEEN [ASC|DESC]]
    [LIMIT <n>] [OFFSET <n>]
    [STATS] [PATHS] [ORPHANS] [CYCLES] [BROKEN] [SUMMARY] [VERBOSE];
```

| Modifikator | Bedeutung |
|---|---|
| `FROM <id>` | Startpunkt: Dokument- oder Benutzer-ID (automatisch erkannt) |
| `TO <id>` | Nur Beziehungen, die diese ID als anderes Ende berühren |
| `DEPTH <n>` | Anzahl der Sprünge ab FROM (Standard 1) |
| `DIRECTION` | `OUT` = Lesevorgänge von einem Benutzer aus; `IN` = Leser hinein zu einem Dokument; `BOTH` (Standard) |
| `FORMAT` | `TABLE` (Standard), `TREE` (benötigt FROM), `GRAPH` (Adjazenzliste), `JSON` |
| `WHERE`/`FILTER` | Bedingung über `doc_id`, `user_id`, `access_count`, `relevance`, `last_seen`, `first_seen` |
| `ORDER BY` | `DEGREE`, `ID`, `NAME`, `ACCESS_COUNT`, `RELEVANCE`, `LAST_SEEN`, `FIRST_SEEN` (+`ASC`/`DESC`) |
| `LIMIT`/`OFFSET` | Paginierung über das endgültige, sortierte Ergebnis |
| `STATS`/`SUMMARY` | Fügt einen zusammenfassenden Bericht voran |
| `PATHS` | Zeigt die FROM-Traversierung als eingerückten Baum |
| `ORPHANS` | Nur isolierte Paare |
| `CYCLES` | Nur Beziehungen, die einen Zyklus schließen |
| `BROKEN` | Nur Beziehungen, deren Dokument später gelöscht wurde |
| `VERBOSE` | Fügt vollständige Metadaten hinzu (first_seen/expires) |

```sql
SHOW AUTORELATIONS produkte;
SHOW AUTORELATIONS produkte FROM p_042;
SHOW AUTORELATIONS produkte FROM p_042 DEPTH 3 DIRECTION BOTH FORMAT TREE;
SHOW AUTORELATIONS produkte STATS;
SHOW AUTORELATIONS produkte WHERE access_count > 10 ORDER BY DEGREE LIMIT 20;
SHOW AUTORELATIONS produkte FROM p145 DEPTH 6 DIRECTION BOTH
  WHERE relevance >= 0.75 ORDER BY ACCESS_COUNT DESC LIMIT 100
  FORMAT TREE STATS VERBOSE;
SHOW AUTORELATIONS produkte CYCLES;
SHOW AUTORELATIONS produkte BROKEN;
```

### Ansichten (VIEWS)

```
VIEW CREATE <name> AS FIND <block> WHERE <bedingung>
VIEW DROP <name>
VIEW SHOW
VIEW INFO <name>
<view_name>                    -- führt die Ansicht aus
```

```sql
VIEW CREATE aktive_benutzer AS FIND benutzer WHERE aktiv = true
VIEW SHOW
VIEW INFO aktive_benutzer
aktive_benutzer
VIEW DROP aktive_benutzer
```

### EXPORT / IMPORT

`EXPORT` schreibt immer **beide** — eine `.csv`- und eine `.json`-Datei —
in `<data_root>/backups/`, mit dem von dir angegebenen Basisnamen. Jede
exportierte Zeile/jedes Dokument enthält sowohl `_id` als auch `id`.

```
EXPORT <block> [WHERE <bedingung>] TO "<datei>"
IMPORT <block> FROM "<datei.json>"        -- sucht auch innerhalb von backups/
IMPORT <block> FROM "<datei.csv>"
```

```sql
EXPORT produkte TO "produkte_export"
-- -> schreibt backups/produkte_export.csv und backups/produkte_export.json
EXPORT produkte WHERE preis > 18 TO "teure_produkte"

IMPORT produkte FROM "produkte_export.json"
IMPORT produkte FROM "produkte_export.csv"
```

### RENAME FIELD — Feld in allen Dokumenten umbenennen

```
RENAME FIELD [<db>] <block>.<feld> TO <neues_feld>
```

Anders als `RENAME BLOCK`/`RENAME DB` (die nur einen Verzeichniseintrag
umbenennen und sofort abgeschlossen sind), ist `RENAME FIELD` ein
**Massen-Rewrite**: jedes Dokument im Block, das `<feld>` besitzt, wird
gelesen, sein Schlüssel geändert und zurückgeschrieben — zusammen mit
dem Sekundärindex und den FLEX-COLUMN-Einträgen dieses Dokuments.

```sql
RENAME FIELD produkt.name TO produktname
RENAME FIELD shop produkt.name TO produktname     -- explizite db
```

**Strukturhinweise:**
- Das erste Argument nach `RENAME FIELD` ist **ein einzelnes Token**
  in der Form `<block>.<feld>` (Block und Feld durch genau einen
  Punkt getrennt); das nächste Token muss `TO` sein, das letzte Token
  ist der neue Feldname.
- Die Datenbank ist optional und steht, falls angegeben, **vor**
  `<block>.<feld>` als eigenes Token — erkannt daran, dass dieses
  Token keinen Punkt enthält (`RENAME FIELD mydb produkt.name TO
  produktname`). Ohne dieses Token wird die aktuelle Datenbank (`USE`)
  verwendet.
- Ein Dokument ohne `<feld>`, oder eines, das bereits `<neues_feld>`
  besitzt (aus einem vorherigen Teillauf oder einer echten
  Namenskollision), bleibt unverändert — kein fataler Fehler, wird
  aber separat als Konflikt gemeldet.
- Die Operation ist **idempotent und fortsetzbar**: sie läuft nicht im
  WAL des ACID-Transaktionsmanagers, sondern als direkte
  Badger-Operation in Batches fester Größe mit einem Cursor. Ein
  Absturz mitten im Lauf hinterlässt einige umbenannte und einige
  nicht umbenannte Dokumente (nicht atomar, kein Alles-oder-nichts) —
  ein erneuter Lauf mit denselben Argumenten setzt die Arbeit dort
  fort, wo sie aufgehört hat, da ein bereits umbenanntes Dokument
  `<feld>` einfach nicht mehr besitzt und übersprungen wird.
- Hält während der Ausführung eine exklusive Sperre pro `<db>/<block>`
  (`rename_field`), damit es nicht mit einem gleichzeitigen `RENAME
  FIELD` auf demselben Block kollidiert.
- Gibt die Anzahl tatsächlich geänderter Dokumente zurück:
  `Field renamed: produkt.name -> produktname in database: shop (128 document(s))`.

### FLEX-COLUMN

Admin-/Diagnose-Unterbefehle für die Flex-Column-Engine (adaptive
Indizierung häufig genutzter Felder).

```
FLEX STATS                     Globale Statistiken der FLEX-COLUMN-Engine
FLEX HOT [<block>]             Erkannte "heiße" Felder (am meisten gelesen/gefiltert)
FLEX REINDEX [<db>] <block>    FLEX-COLUMN-Index eines Blocks neu aufbauen
```

```sql
FLEX STATS
FLEX HOT produkte
FLEX REINDEX produkte
FLEX REINDEX shop produkte
```

### Benutzerverwaltung

```
CREATE USER <name> IDENTIFIED BY "<passwort>"
    [ROLE <ADMIN|SUBADMIN|DEVELOPER|USER>]
    [STATUS <ACTIVE|DISABLED>]
    [DEFAULT DATABASE <db>]
    [DEFAULT BLOCK <block>]
    [PASSWORD EXPIRE <NEVER|DAYS <n>>]
    [ACCOUNT LOCK <ON|OFF>]
    [COMMENT "<beschreibung>"]
DROP USER <name>
SHOW USERS

CREATE SERVER <name> ADMIN [<benutzer>] IDENTIFIED BY "<passwort>"
```

**Strukturhinweise — `CREATE USER`:**
- Alle Klauseln nach dem Benutzernamen sind optional außer
  `IDENTIFIED BY`, und können **in beliebiger Reihenfolge** angegeben
  werden.
- Die alte Kurzform wird ebenfalls unterstützt: `CREATE USER <user>
  "<pass>" [ROLE <role>]` (Passwort positional statt `IDENTIFIED BY`).
- `ROLE` ist standardmäßig `USER`, falls weggelassen.
- `PASSWORD EXPIRE DAYS <n>` erfordert eine Ganzzahl `>= 0`;
  `PASSWORD EXPIRE NEVER` deaktiviert das Ablaufen.

**Strukturhinweise — `CREATE SERVER`:**
- Erstellt einen neuen logischen Server: ein eigenes Datenverzeichnis,
  isoliert von anderen Servern, mit eigenem Admin-Benutzer — alle
  Server teilen sich denselben Port/Prozess; unterscheidend sind nur
  die Admin-Zugangsdaten.
- `<benutzer>` (der Admin-Name für diesen Server) ist optional; wird
  er weggelassen, wird `admin` verwendet.

```sql
CREATE USER analyst IDENTIFIED BY "s3cret!" ROLE readonly
CREATE USER carla IDENTIFIED BY "andereClave!" ROLE DEVELOPER STATUS ACTIVE
    DEFAULT DATABASE shop DEFAULT BLOCK produkte
    PASSWORD EXPIRE DAYS 90 ACCOUNT LOCK OFF
    COMMENT "CI-Integrationskonto"
SHOW USERS
DROP USER analyst

CREATE SERVER filiale_nord ADMIN IDENTIFIED BY "adminPass!"
CREATE SERVER filiale_sued ADMIN admin_sued IDENTIFIED BY "adminPass2!"
```

### Zugriffskontrolle (GRANT / REVOKE / SHOW GRANTS)

```
GRANT read[,write] ON <db>.<block> TO <benutzer>
REVOKE read[,write] ON <db>.<block> FROM <benutzer>
SHOW GRANTS [FOR <benutzer>]
```

```sql
GRANT read ON shop.products TO analyst
GRANT read,write ON shop.* TO carla        -- alle Blöcke in shop
GRANT read ON shop TO alice                -- db ohne Punkt == shop.* (äquivalent)
REVOKE write ON shop.products FROM carla
SHOW GRANTS
SHOW GRANTS FOR analyst
```

- `<block>` kann `*` sein für "alle Blöcke in `<db>`"; ein bloßes `<db>`
  ohne Punkt nach `ON` wird genauso behandelt wie `<db>.*`.
- Nur ein Administrator (Rolle `ADMIN` oder `ADMIN_GENERAL`) darf
  `GRANT` oder `REVOKE` ausführen. Ein begrenzter `ADMIN` (nicht
  `ADMIN_GENERAL`) kann Zugriff nur innerhalb seiner eigenen Datenbank
  gewähren/entziehen — dieselbe Isolation, die `USE`/`CD` an anderer
  Stelle durchsetzen.
- Ein granularer `GRANT` auf einen bestimmten Block genügt, damit dieser
  Benutzer per `USE`/`CD` in die Datenbank wechseln kann, selbst wenn
  seine Rolle allein das nicht erlauben würde; die Prüfung pro
  Block/Aktion greift weiterhin, sobald er einen Block tatsächlich
  anfasst.
- `SHOW GRANTS` ohne `FOR` zeigt die eigenen Berechtigungen des
  Aufrufers. `SHOW GRANTS FOR <benutzer>` für einen anderen Benutzer
  erfordert Administratorrechte.

### Shard-Verwaltung

```
SHARD STATUS
SHARD REBALANCE
SHARD SCALE <db> <shards>
```

```sql
SHARD STATUS
SHARD REBALANCE
SHARD SCALE shop 32
```

### Cluster

```
CLUSTER STATUS
```

### CHECKPOINT

```
CHECKPOINT        Erzwingt einen Checkpoint: synchronisiert alle Datenbanken
                  auf die Festplatte und rotiert ggf. das WAL
```

```sql
CHECKPOINT
-- -> Checkpoint completed: 3 database(s) synced, WAL rotated: true, took 42ms
```

### Navigation & System

```
PWD               Aktuellen Pfad anzeigen
LS                Datenbanken auflisten
LS <db>           Blöcke in einer Datenbank auflisten
CD <db>           Zu einer Datenbank wechseln
TREE              Vollständigen Verzeichnisbaum anzeigen
STATUS            Systemstatus anzeigen
HEALTH            Gesundheitscheck anzeigen
VERSION           Version anzeigen
PING              Die Engine anpingen
HELP              Hilfe anzeigen
EXIT, QUIT        Die Konsole verlassen
```

### Filteroperatoren (in jedem `WHERE` verwendbar)

| Operator | Bedeutung |
|---|---|
| `=`, `==` | Gleich |
| `!=`, `<>` | Ungleich |
| `>`, `<` | Größer als / Kleiner als |
| `>=`, `<=` | Größer oder gleich / Kleiner oder gleich |
| `LIKE` | Mustervergleich (Platzhalter `%`) |
| `CONTAINS` | Enthält Teilzeichenkette |
| `EXISTS` | Feld existiert |
| `IN` | Wert in Liste |
| `NOT IN` | Wert nicht in Liste |
| `BETWEEN` | Bereich zwischen zwei Werten |
| `STARTS WITH` | Text beginnt mit |
| `ENDS WITH` | Text endet mit |
| `AND` | Logisches UND (Standard, wenn weggelassen) |
| `OR` | Logisches ODER |

### Vollständiges Beispiel

```sql
CREATE DB shop
USE shop
CREATE BLOCK produkte

INSERT produkte kb001 {"name": "Tastatur", "preis": 49.90, "auf_lager": true}
INSERT produkte {"name": "Maus", "preis": 19.90, "auf_lager": true}
INSERT produkte name: "Monitor", preis: 199.00, auf_lager: true

FIND produkte WHERE preis > 20
FIND produkte WHERE name LIKE "%tatur%" ORDER preis:DESC
FIND produkte WHERE preis BETWEEN 20 AND 200 SELECT name, preis

SEARCH produkte "tastatur" WITH SCORE

UPDATE produkte WHERE _id = "kb001" SET preis = 45
UPDATE produkte WHERE preis < 30 INC preis = 1

COUNT produkte WHERE auf_lager = true
AVG produkte preis
GROUP produkte BY auf_lager COUNT

DELETE produkte WHERE _id = "kb001"

BEGIN shop produkte
  INSERT produkte {"name": "Webcam", "preis": 39.90}
  UPDATE produkte WHERE name = "Maus" SET preis = 17.90
COMMIT

VIEW CREATE guenstige_produkte AS FIND produkte WHERE preis < 25
guenstige_produkte

EXPORT produkte TO "produkte_sicherung"
STATUS
STATS DB shop
```


## Vollständiger Befehlsindex

Die Query-Engine verarbeitet die folgenden Befehle auf oberster Ebene. Die vorherigen Abschnitte definieren ihre Argumente und Varianten.

| Command | Syntax |
|---|---|
| `VERSION` | `VERSION` |
| `PING` | `PING` |
| `STATUS` | `STATUS` |
| `HEALTH` | `HEALTH` |
| `HELP` | `HELP` |
| `EXIT / QUIT` | `EXIT | QUIT | \Q` |
| `PWD` | `PWD` |
| `CD` | `CD <db>` |
| `LS` | `LS [<db>]` |
| `TREE` | `TREE` |
| `CREATE` | `CREATE DB <db> | CREATE BLOCK [<db>] <block> | CREATE USER ... | CREATE INDEX ...` |
| `DROP` | `DROP DB <db> | DROP BLOCK <block> | DROP FIELD <block>.<field> | DROP INDEX ...` |
| `RENAME` | `RENAME DB <old> TO <new> | RENAME BLOCK ...` |
| `GRANT` | `GRANT <actions> ON <db>.<block> TO <user>` |
| `REVOKE` | `REVOKE <actions> ON <db>.<block> FROM <user>` |
| `SHOW` | `SHOW DBS | SHOW BLOCKS ... | SHOW USERS | SHOW GRANTS [FOR <user>] | SHOW INDEXES ON <block> | SHOW ...` |
| `INFO` | `INFO DB <db> | INFO BLOCK <block>` |
| `DESCRIBE` | `DESCRIBE DB <db> | DESCRIBE BLOCK <block>` |
| `STATS` | `STATS [DB|BLOCK] [<name>]` |
| `SIZE` | `SIZE [DB|BLOCK] [<name>]` |
| `USE` | `USE <db>` |
| `INSERT` | `INSERT <block> <json> | INSERT <block> SET <field>=<value>[, ...] | INSERT ...` |
| `FIND` | `FIND <block> [WHERE <expr>] [ORDER <field>[:ASC|DESC]] [LIMIT <n>] [OFFSET <n>] [DISTINCT <field>]` |
| `EXPLAIN` | `EXPLAIN FIND ... | EXPLAIN SEARCH ...` |
| `GET` | `GET <block> <id>` |
| `SEARCH` | `SEARCH <block> <text> [WHERE ...] [LIMIT ...]` |
| `UPDATE` | `UPDATE <block> WHERE <expr> SET <field>=<value>[, ...] | UPDATE ALL <block> SET ...` |
| `DELETE` | `DELETE <block> WHERE <expr> | DELETE ALL <block>` |
| `CLEAR / EMPTY` | `CLEAR <block> | EMPTY <block>` |
| `COUNT` | `COUNT <block> [WHERE <expr>]` |
| `SUM / AVG / MIN / MAX / MEDIAN / MODE / STDDEV` | `<AGGREGATE> <block> <field> [WHERE <expr>]` |
| `GROUP` | `GROUP <block> BY <field>[, ...] [WHERE <expr>] [HAVING <expr>]` |
| `JOIN` | `JOIN <block> WITH <block> ON <field>=<field> ...` |
| `RELATE` | `RELATE ...` |
| `EXPORT` | `EXPORT <block> TO <file> [FORMAT ...]` |
| `IMPORT` | `IMPORT <block> FROM FILE <file> [FORMAT ...] [BATCH ...] | IMPORT <block> <file>` |
| `BACKUP` | `BACKUP <db> [FULL] | BACKUP <db> <file>` |
| `RESTORE` | `RESTORE <db> [AS OF <timestamp>] | RESTORE <db> FROM <file>` |
| `VERIFY BACKUP` | `VERIFY BACKUP <db>` |
| `COMPACT` | `COMPACT <db>` |
| `CHECKPOINT` | `CHECKPOINT` |
| `ANALYZE` | `ANALYZE DB|BLOCK ...` |
| `OPTIMIZE` | `OPTIMIZE DB|BLOCK ...` |
| `REBUILD` | `REBUILD BLOCK [<db>] <block>` |
| `CHECK` | `CHECK ...` |
| `REPAIR` | `REPAIR ...` |
| `SHARD` | `SHARD ...` |
| `CLUSTER` | `CLUSTER ...` |
| `VIEW` | `VIEW ...` |
| `FLEX` | `FLEX ...` |
| `BEGIN` | `BEGIN [<db>] [<block>]` |
| `COMMIT` | `COMMIT` |
| `ROLLBACK / ABORT` | `ROLLBACK | ABORT` |
| `TX / TRANSACTION` | `TX [STATUS|LIST|STATS] | TRANSACTION [STATUS|LIST|STATS]` |
| `BULK` | `BULK ...` |
| `TRUNCATE` | `TRUNCATE <block>` |
| `UPSERT` | `UPSERT <block> <id> SET <field>=<value>[, ...] | UPSERT <block> <id> <json>` |
| `ADD FIELD` | `ADD FIELD <block>.<field> DEFAULT <value> [, <field2> DEFAULT <value2> ...]` |
