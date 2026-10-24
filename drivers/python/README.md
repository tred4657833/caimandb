# caimandb-client (Python)

Cliente sin dependencias externas para CaimanDB (solo usa `urllib` de
la librería estándar). Compatible con Python 3.8+.

## Uso

```python
from caimandb import CaimanDBClient

db = CaimanDBClient(
    query_url="http://localhost:1555",
    admin_url="http://localhost:1556",
)

# Opción A: login con usuario/contraseña (obtiene y guarda el JWT)
db.login("admin", "secret")

# Opción B: Basic Auth directo, sin login
# db = CaimanDBClient(username="admin", password="secret")

# Opción C: token ya obtenido
# db = CaimanDBClient(token="eyJ...")

db.insert("users", {"name": "John", "age": 30})

res = db.find(
    "users",
    where='age > 18 AND status = "active"',
    select=["name", "age"],
    order="age:DESC",
    limit=50,
)
print(res["result"])

db.update("users", '_id = "abc"', "SET age = 31")
db.delete("users", "age < 18")

# Comando NQL crudo, para lo que no tenga wrapper propio
db.query("CREATE BLOCK users")

# Change stream en tiempo real (bloqueante -- correr en un hilo aparte
# si no querés bloquear el hilo principal)
db.watch(lambda event: print(event), db="default", block="users")
```

## API

- `login(username, password)`
- `query(nql, db=None)` — ejecuta cualquier comando NQL crudo
- `insert(block, doc_or_docs, db=None)`
- `get(block, doc_id, db=None)`
- `find(block, where=None, select=None, order=None, limit=None, offset=None, db=None)`
- `search(block, text, exact=False, fuzzy=False, with_score=False, with_matches=False, db=None)`
- `update(block, where, set_clause, db=None)`
- `delete(block, where, db=None)`
- `count(block, where=None, db=None)`
- `health()`, `status()`
- `watch(on_event, db=None, block=None)` — bloqueante, un callback por evento

Ver `docs/nql-reference.md` en el repo de CaimanDB para la sintaxis
completa de NQL — este cliente expone wrappers para lo más común, pero
`query()` acepta cualquier comando.
