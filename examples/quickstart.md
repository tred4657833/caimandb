# Quick Start Guide

## 1. Build and start

```bash
go build ./cmd/caimandb
./caimandb
```

On first startup, if `configs/caimandb.conf` doesn't exist, CaimanDB
creates one with default values (also creating the `configs/` folder
if necessary). You can use
[`configs/caimandb.conf.example`](../configs/caimandb.conf.example) instead,
by copying it as `configs/caimandb.conf`.

## 2. Create a database and a block

```sql
CREATE DB shop
USE shop
CREATE BLOCK products
```

## 3. Insert documents

```sql
INSERT products {"name": "Mechanical Keyboard", "price": 45.99, "stock": 120}
INSERT products {"name": "Wireless Mouse", "price": 19.99, "stock": 300}
```

## 4. Query

```sql
FIND products WHERE price < 30
FIND products SELECT name, price ORDER price:DESC
COUNT products WHERE stock > 0
```

## 5. Transaction

```sql
BEGIN shop products
UPDATE products WHERE name = "Wireless Mouse" INC stock = -1
COMMIT
```

## 6. Via HTTP

```bash
curl -u admin:change-me -X POST http://localhost:1555/query \
  -d 'FIND products WHERE price < 30'
```

More commands: [`docs/nql-reference.md`](../docs/nql-reference.md).
More endpoints: [`docs/api/http-api.md`](../docs/api/http-api.md).
