# Administración jerárquica y aislada

CaimanDB cuenta con un sistema de administración jerárquico y aislado,
donde existe un **Admin General** con control global y **administradores
independientes** responsables de sus propias bases. Cada base identifica
en sus metadatos a su administrador, garantizando que solo los usuarios
autorizados puedan acceder a ella. Cada administrador puede crear y
gestionar subadmins, desarrolladores y usuarios dentro de su propio
entorno, manteniendo los datos completamente separados entre bases.

```text
ADMIN GENERAL
│
├── BASE A → ADMIN A
│            ├── SUBADMIN
│            ├── DEV
│            └── USUARIOS
│
├── BASE B → ADMIN B
│            ├── SUBADMIN
│            ├── DEV
│            └── USUARIOS
```

* **Admin General** (`ADMIN_GENERAL`): acceso a todas las bases y usuarios.
* **Admin** (`ADMIN`): solo puede acceder y administrar su propia base.
* **Subadmin / Dev / Usuario**: acceso limitado dentro de su entorno.
* Cada base guarda en sus metadatos qué administrador la gestiona.
* Un `ADMIN` puede crear `SUBADMIN`, `DEVELOPER` y `USER` dentro de su
  propia base.
* Aislamiento: ningún admin puede acceder a otra base salvo el Admin
  General.

Regla: `Identidad → Rol → Base/Scope → Permisos → Acceso`.

## Modelo de datos

* **Roles** (`internal/caimandb/roles.go`): `ADMIN_GENERAL`, `ADMIN`,
  `SUBADMIN`, `DEVELOPER`, `USER`. `IsGeneralAdmin(role)` distingue al
  único rol sin base propia; `IsAdminRole(role)` es verdadero para
  `ADMIN_GENERAL` y para `ADMIN` (privilegio de administrador, sin
  importar el alcance).
* **`User.Database`** (`internal/caimandb/users_auth.go`): la base a la
  que pertenece el usuario. Obligatoria para todo rol salvo
  `ADMIN_GENERAL` (que no debe indicarla). Se valida en
  `CreateUserWithOpts`.
* **Dueño de la base**: se guarda en los metadatos ya existentes de cada
  base (`db.json`, vía `storage.DirectoryManager.Save/LoadDBMeta`), bajo
  la clave `"admin"`. Ver `Engine.SetDBOwner` / `Engine.GetDBOwner` en
  `internal/caimandb/hierarchy.go`.
* **`Session.authDatabase`** (`internal/caimandb/dsl_parser.go`): la
  base ligada a la identidad autenticada de la sesión (viene de
  `User.Database` / `TokenClaims.Database`). No cambia aunque
  `Session.currentDB` sí lo haga con `USE`/`CD`.

## Sintaxis

```sql
-- El Admin General crea una base y, opcionalmente, le asigna dueño:
CREATE DB ventas OWNER admin_ventas

-- El Admin General da de alta al ADMIN dueño de esa base:
CREATE USER admin_ventas IDENTIFIED BY "..." ROLE ADMIN DATABASE ventas

-- Ese ADMIN, ya autenticado y limitado a "ventas", da de alta a su equipo
-- (DATABASE se hereda automáticamente si se omite):
CREATE USER dev1 IDENTIFIED BY "..." ROLE DEVELOPER
CREATE USER lector1 IDENTIFIED BY "..." ROLE USER DATABASE ventas
```

## Autorización implementada

* `Engine.CreateUserWithOpts` (defensa en profundidad, aplica siempre):
  exige `DATABASE` para todo rol salvo `ADMIN_GENERAL` y lo rechaza para
  `ADMIN_GENERAL`.
* `authorizeCreateUser` (`hierarchy.go`), invocada desde `CREATE USER`
  (consola y HTTP `/api/v1/users`): un `ADMIN`/`SUBADMIN` autenticado
  solo puede crear `SUBADMIN`/`DEVELOPER`/`USER`, y solo dentro de su
  propia base; nunca otro `ADMIN` ni un `ADMIN_GENERAL`. La consola
  local sin autenticar (`Session.authRole == ""`, el REPL de arranque)
  conserva el comportamiento histórico de operador ya confiado. La
  comparación de rol es insensible a mayúsculas/minúsculas (se corrigió
  un bug donde `sess.authRole == RoleAdmin` fallaba en crudo contra un
  rol en minúsculas).
* `authorizeDropUser` (`hierarchy.go`), invocada desde `DROP USER`
  (consola): misma regla que `authorizeCreateUser`, pero para borrar.
  Un `ADMIN`/`SUBADMIN` solo puede borrar `SUBADMIN`/`DEVELOPER`/`USER`
  dentro de su propia base -- nunca otro `ADMIN`, nunca al
  `ADMIN_GENERAL`, y nunca a nadie fuera de su base. Si el usuario
  objetivo no existe o no se puede leer, se deniega igual (fail closed)
  en vez de dejar que `DropUser` falle por su cuenta, para no filtrar
  por el mensaje de error si una cuenta existe en otra base.
* `enforceSessionIsolation` (`hierarchy.go`), invocada al inicio de
  `parseAndExec` para toda sesión autenticada que no sea
  `ADMIN_GENERAL`: rechaza cualquier comando si `Session.currentDB`
  apunta a una base distinta de `Session.authDatabase`. Cubre el
  parámetro `db` de los endpoints HTTP (`/api/v1/query`, `/query`) y los
  comandos `USE`/`CD` (que además verifican el destino antes de
  cambiar de base).
* `requireDBAccess` (`hierarchy.go`): el mismo gate fail-closed que
  `enforceSessionIsolation`, pero para comandos que reciben el nombre de
  la base como argumento explícito en vez de depender de `currentDB`.
  Cierra por construcción ante cualquier duda -- base vacía, sesión sin
  `authDatabase`, o simple mismatch -- todo deniega, sin rama que
  "continúe". Insertado en:
  * `cmd_admin.go`: `ANALYZE DB/BLOCK`, `OPTIMIZE DB/BLOCK`, `BACKUP`,
    `RESTORE`, `COMPACT`, `SHARD SCALE`.
  * `cmd_admin_extra.go`: `resolveDBBlock` (gate único para `DROP BLOCK`,
    `INFO`/`DESCRIBE BLOCK`, `SIZE BLOCK`, `REBUILD BLOCK`,
    `CHECK BLOCK`, `REPAIR BLOCK`, `FLEX REINDEX`), `DROP DB`,
    `RENAME DB`, `RENAME BLOCK` (forma simple y forma con lista entre
    paréntesis, vía `renameBlocks`), `RENAME FIELD` (vía `renameField`),
    `STATS DB`, `SIZE DB`, `INFO`/`DESCRIBE DB`, `BEGIN <db>`.
  * `cmd_create_show.go`: `CREATE BLOCK` (forma simple y forma con lista,
    vía `createBlocks`), `SHOW INDEXES`.
  * `cmd_show_size.go`: `SHOW BLOCKS <db>`.
  * `http_admin.go`: `POST /api/v1/dbs`, `GET`/`DELETE /api/v1/db/{name}`,
    `POST /api/v1/backup`, `/api/v1/restore`, `/api/v1/compact`. Estos
    endpoints no tenían **ninguna** comprobación de aislamiento antes de
    este cambio -- cualquier usuario autenticado, sin importar su rol,
    podía describir/borrar cualquier base o respaldar/restaurar/compactar
    cualquier base por nombre.
  * `http_query.go`: `GET /entities` tenía el mismo problema (listaba
    todas las bases y los bloques de cualquier base por query param `db`
    sin comprobación alguna); ahora aplica `requireDBAccess` sobre `db` y
    filtra la lista de bases a la propia, igual que `SHOW DBS`.
* `SHOW DBS` y `SHOW USERS` (consola) y `GET /api/v1/users` filtran sus
  resultados a la base de quien pregunta (`ShowUsersScoped`), salvo para
  `ADMIN_GENERAL`. `GET /api/v1/dbs` y `GET /entities` ahora hacen lo
  mismo (ver arriba).
* `CREATE DB`: un `ADMIN` solo puede crear la base que ya tiene asignada
  como propia y queda registrado automáticamente como su dueño; el
  `ADMIN_GENERAL` puede crear cualquier base y asignar dueño con
  `OWNER <usuario>`. Aplica tanto en consola como en `POST /api/v1/dbs`.
* El admin de arranque (`app.go`, primera vez) y el admin de
  `CREATE SERVER` (`server_manager.go`) ahora se crean como
  `ADMIN_GENERAL`.

## Pruebas de aislamiento

`internal/caimandb/hierarchy_isolation_test.go` cubre:

* `TestUserCanAccessDB_FailsClosed` / `TestRequireDBAccess_FailsClosed`:
  el gate a nivel de función, incluyendo los casos de duda (base vacía,
  sesión sin `authDatabase`, rol en minúsculas).
* `TestIsolation_CrossTenantCommandsDenied`: cada uno de los ~18 comandos
  listados arriba, ejecutado por el `ADMIN` de un tenant contra la base
  del otro tenant -- todos deben devolver `ERROR`.
* `TestIsolation_SameTenantCommandsAllowed`: control positivo, los
  mismos comandos sobre la propia base no deben quedar bloqueados.
* `TestIsolation_GeneralAdminBypassesAllGates`: el `ADMIN_GENERAL` sigue
  operando sobre cualquier base.
* `TestIsolation_DropUserScoped`: un `ADMIN` puede borrar usuarios de su
  propia base, pero no de otra base ni a otro `ADMIN`.

## Siguiente fase

Con lo anterior, todos los puntos de entrada identificados que reciben
un nombre de base -- ya sea vía `currentDB` o como argumento explícito,
en consola o en HTTP -- pasan por el mismo gate fail-closed. Pendientes
razonables para una siguiente fase, sin relación con aislamiento:

* Migración de instalaciones existentes: un `ADMIN` creado antes de este
  cambio no tiene `Database` -- `CreateUserWithOpts` ya no lo permitiría
  crear de nuevo así, pero los ya existentes en disco no se tocan
  automáticamente. Conviene un comando `ALTER USER ... DATABASE <base>`
  (no existe todavía) para asignarles base sin recrearlos.
  Alternativa: normalizar en el arranque, promoviendo el primer `ADMIN`
  legado encontrado a `ADMIN_GENERAL`.
* Si en algún momento se permite que un `User.Role` quede vacío para un
  usuario ya autenticado por HTTP (hoy no debería ocurrir, pero no hay
  una comprobación explícita que lo impida en `resolveAdmin`), ese
  caso caería en la misma rama de "consola local sin autenticar" de
  `requireDBAccess`/`enforceSessionIsolation` y pasaría sin comprobar.
  Vale la pena, en la siguiente fase, decidir si `TokenClaims.Role`
  vacío debería denegar explícitamente en vez de heredar ese atajo
  pensado para el REPL de arranque.
