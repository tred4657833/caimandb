# caimandb-client (Node.js)

Cliente sin dependencias externas para CaimanDB. Requiere Node 18+
(usa el `fetch` incorporado).

## Uso

```js
const { CaimanDBClient } = require('./caimandb');

const db = new CaimanDBClient({
  queryURL: 'http://localhost:1555',
  adminURL: 'http://localhost:1556',
});

// Opción A: login con usuario/contraseña (obtiene y guarda el JWT)
await db.login('admin', 'secret');

// Opción B: Basic Auth directo, sin login
// const db = new CaimanDBClient({ username: 'admin', password: 'secret' });

// Opción C: token ya obtenido
// const db = new CaimanDBClient({ token: 'eyJ...' });

await db.insert('users', { name: 'John', age: 30 });

const res = await db.find('users', {
  where: 'age > 18 AND status = "active"',
  select: ['name', 'age'],
  order: 'age:DESC',
  limit: 50,
});
console.log(res.result);

await db.update('users', '_id = "abc"', 'SET age = 31');
await db.delete('users', 'age < 18');

// Comando NQL crudo, para lo que no tenga wrapper propio
await db.query('CREATE BLOCK users');

// Change stream en tiempo real
const watcher = db.watch((event) => console.log(event), { db: 'default', block: 'users' });
// watcher.abort() para cortar la suscripción
```

## API

- `login(username, password)`
- `query(nql, db?)` — ejecuta cualquier comando NQL crudo
- `insert(block, docOrDocs, db?)`
- `get(block, id, db?)`
- `find(block, { where, select, order, limit, offset }, db?)`
- `search(block, text, { exact, fuzzy, withScore, withMatches }, db?)`
- `update(block, where, setClause, db?)`
- `delete(block, where, db?)`
- `count(block, where?, db?)`
- `health()`, `status()`
- `watch(onEvent, { db?, block? })` → devuelve un `AbortController`

Ver `docs/nql-reference.md` en el repo de CaimanDB para la sintaxis
completa de NQL (WHERE, JOIN, agregaciones, transacciones, etc.) — este
cliente expone wrappers para lo más común, pero `query()` acepta
cualquier comando.
