# caimandb/caimandb-client (PHP)

Cliente sin dependencias externas para CaimanDB (solo requiere las
extensiones `curl` y `json`, incluidas en la mayoría de instalaciones
de PHP). Compatible con PHP 7.4+.

> **Nota:** este driver se escribió sin un intérprete de PHP disponible
> en el entorno para probarlo. La sintaxis se revisó a mano con
> cuidado, pero corré `php -l src/CaimanDBClient.php` (lint) antes de
> usarlo en algo serio, y avisame si sale algún error para corregirlo.

## Uso

```php
<?php
require 'vendor/autoload.php'; // o require 'src/CaimanDBClient.php';

use CaimanDB\CaimanDBClient;

$db = new CaimanDBClient(
    queryUrl: 'http://localhost:1555',
    adminUrl: 'http://localhost:1556',
);

// Opción A: login con usuario/contraseña (obtiene y guarda el JWT)
$db->login('admin', 'secret');

// Opción B: Basic Auth directo, sin login
// $db = new CaimanDBClient(username: 'admin', password: 'secret');

// Opción C: token ya obtenido
// $db = new CaimanDBClient(token: 'eyJ...');

$db->insert('users', ['name' => 'John', 'age' => 30]);

$res = $db->find(
    'users',
    where: 'age > 18 AND status = "active"',
    select: ['name', 'age'],
    order: 'age:DESC',
    limit: 50,
);
print_r($res['result']);

$db->update('users', '_id = "abc"', 'SET age = 31');
$db->delete('users', 'age < 18');

// Comando NQL crudo, para lo que no tenga wrapper propio
$db->query('CREATE BLOCK users');

// Change stream en tiempo real (bloqueante -- devolver `false` desde
// el callback corta la suscripción)
$db->watch(function (array $event) {
    print_r($event);
}, db: 'default', block: 'users');
```

## API

- `login(string $username, string $password)`
- `query(string $nql, ?string $db = null)` — ejecuta cualquier comando NQL crudo
- `insert(string $block, array $docOrDocs, ?string $db = null)`
- `get(string $block, string $id, ?string $db = null)`
- `find(string $block, ?string $where, ?array $select, ?string $order, ?int $limit, ?int $offset, ?string $db)`
- `search(string $block, string $text, bool $exact, bool $fuzzy, bool $withScore, bool $withMatches, ?string $db)`
- `update(string $block, string $where, string $setClause, ?string $db = null)`
- `delete(string $block, string $where, ?string $db = null)`
- `count(string $block, ?string $where = null, ?string $db = null)`
- `health()`, `status()`
- `watch(callable $onEvent, ?string $db = null, ?string $block = null)` — bloqueante

Ver `docs/nql-reference.md` en el repo de CaimanDB para la sintaxis
completa de NQL — este cliente expone wrappers para lo más común, pero
`query()` acepta cualquier comando.
