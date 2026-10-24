# caimandb-client (Java)

Cliente sin dependencias externas para CaimanDB. Usa únicamente
`java.net.http.HttpClient` (parte del JDK desde Java 11) y un
encoder/decoder JSON propio y minimalista (`Json.java`) para no
depender de Jackson/Gson.

> Este driver se verificó compilando y corriendo su lógica (incluyendo
> el armado de comandos NQL para `find`/`insert`/`update`) contra un
> puerto inexistente, confirmando que compila y que la lógica hasta la
> llamada de red funciona. No se hizo una prueba de integración contra
> un servidor CaimanDB real.

## Uso

```java
import com.caimandb.client.CaimanDBClient;
import java.util.Map;

CaimanDBClient.Options opts = new CaimanDBClient.Options();
opts.queryUrl = "http://localhost:1555";
opts.adminUrl = "http://localhost:1556";
CaimanDBClient db = new CaimanDBClient(opts);

// Opción A: login con usuario/contraseña (obtiene y guarda el JWT)
db.login("admin", "secret");

// Opción B: Basic Auth directo, sin login
// opts.username = "admin"; opts.password = "secret";

// Opción C: token ya obtenido
// opts.token = "eyJ...";

db.insert("users", Map.of("name", "John", "age", 30));

CaimanDBClient.FindOptions fo = new CaimanDBClient.FindOptions();
fo.where = "age > 18 AND status = \"active\"";
fo.select = java.util.List.of("name", "age");
fo.order = "age:DESC";
fo.limit = 50;
Map<String, Object> res = db.find("users", fo);
System.out.println(res.get("result"));

db.update("users", "_id = \"abc\"", "SET age = 31");
db.delete("users", "age < 18");

// Comando NQL crudo, para lo que no tenga wrapper propio
db.query("CREATE BLOCK users");

// Change stream en tiempo real (bloqueante -- correr en su propio
// hilo si no querés bloquear el actual)
new Thread(() -> {
    try {
        db.watch(event -> System.out.println(event), "default", "users");
    } catch (Exception e) {
        e.printStackTrace();
    }
}).start();
```

## API

- `login(String username, String password)`
- `query(String nql, String db)` / `query(String nql)` — ejecuta cualquier comando NQL crudo
- `insert(String block, Object docOrDocs, String db)` (acepta `Map`/`List<Map>`)
- `get(String block, String id, String db)`
- `find(String block, FindOptions opts, String db)`
- `search(String block, String text, boolean exact, boolean fuzzy, boolean withScore, boolean withMatches, String db)`
- `update(String block, String where, String setClause, String db)`
- `delete(String block, String where, String db)`
- `count(String block, String where, String db)`
- `health()`, `status()`
- `watch(Consumer<Map<String,Object>> onEvent, String db, String block)` — bloqueante

Ver `docs/nql-reference.md` en el repo de CaimanDB para la sintaxis
completa de NQL — este cliente expone wrappers para lo más común, pero
`query()` acepta cualquier comando.

## Build

```
mvn package
```
