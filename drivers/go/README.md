# caimandb-client-go

Cliente sin dependencias externas para CaimanDB (solo librería
estándar). Requiere Go 1.21+.

> **Nota:** este driver se escribió sin un toolchain de Go disponible
> para compilarlo/verificarlo en el momento. Sintaxis y tipos fueron
> revisados a mano con cuidado, pero corré `go build ./...` y `go vet
> ./...` antes de usarlo en algo serio, y avisame si sale algún error
> para corregirlo.

## Uso

```go
package main

import (
	"fmt"
	"log"

	"github.com/caimandb/caimandb-client-go/caimandb"
)

func main() {
	db := caimandb.New(caimandb.Options{
		QueryURL: "http://localhost:1555",
		AdminURL: "http://localhost:1556",
	})

	// Opción A: login con usuario/contraseña (obtiene y guarda el JWT)
	if _, err := db.Login("admin", "secret"); err != nil {
		log.Fatal(err)
	}

	// Opción B: Basic Auth directo, sin login
	// db := caimandb.New(caimandb.Options{Username: "admin", Password: "secret"})

	// Opción C: token ya obtenido
	// db := caimandb.New(caimandb.Options{Token: "eyJ..."})

	if _, err := db.Insert("users", map[string]any{"name": "John", "age": 30}, ""); err != nil {
		log.Fatal(err)
	}

	res, err := db.Find("users", caimandb.FindOptions{
		Where:  `age > 18 AND status = "active"`,
		Select: []string{"name", "age"},
		Order:  "age:DESC",
		Limit:  50,
	}, "")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res["result"])

	db.Update("users", `_id = "abc"`, "SET age = 31", "")
	db.Delete("users", "age < 18", "")

	// Comando NQL crudo, para lo que no tenga wrapper propio
	db.Query("CREATE BLOCK users", "")

	// Change stream en tiempo real (bloqueante -- correr en su propia
	// goroutine si no querés bloquear el hilo principal)
	go db.Watch(func(ev caimandb.ChangeEvent) {
		fmt.Printf("%+v\n", ev)
	}, "default", "users")
}
```

## API

- `Login(username, password string)`
- `Query(nql, db string)` — ejecuta cualquier comando NQL crudo (`db` vacío usa el default)
- `Insert(block string, docOrDocs any, db string)`
- `Get(block, id, db string)`
- `Find(block string, opts FindOptions, db string)`
- `Search(block, text string, opts SearchOptions, db string)`
- `Update(block, where, setClause, db string)`
- `Delete(block, where, db string)`
- `Count(block, where, db string)`
- `Health()`, `Status()`
- `Watch(onEvent func(ChangeEvent), db, block string)` — bloqueante

Ver `docs/nql-reference.md` en el repo de CaimanDB para la sintaxis
completa de NQL — este cliente expone wrappers para lo más común, pero
`Query()` acepta cualquier comando.
