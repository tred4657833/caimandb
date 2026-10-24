// cmd_tx_dml.go conecta las sentencias INSERT/UPDATE/DELETE del DSL con
// la transacción abierta en la sesión (BEGIN ... COMMIT/ROLLBACK).
//
// Antes de esto, BEGIN creaba la transacción y la guardaba en
// Session.currentTx, pero ningún handler la miraba: handleInsert,
// handleUpdate y handleDelete escribían directo contra el motor. El
// resultado era que un ROLLBACK no deshacía nada -- los cambios ya
// estaban aplicados y comiteados uno por uno -- aunque help.go y
// docs/NQL_SYNTAX.en.md documentan justamente lo contrario
// ("Rollback (discard all changes)").
//
// Las funciones exec* de este archivo son el único punto por el que
// esos tres comandos escriben ahora: si hay una transacción activa
// ligada al mismo <db>.<block>, la escritura se acumula como mutación
// en TxManager (que ya implementaba Insert/Update/Delete pero no tenía
// quién lo llamara desde la consola) y se aplica atómicamente en
// COMMIT; si no la hay, se llama exactamente a la misma API del motor
// que se llamaba antes, así que el camino sin transacción no cambia.
package caimandb

import (
	"fmt"

	"caimandb/internal/caimandb/parse"
)

// txFor devuelve la transacción activa de la sesión cuando la sentencia
// apunta al mismo <db>.<block> con el que se abrió, nil cuando no hay
// ninguna abierta, y error cuando hay una abierta pero apunta a otro
// sitio.
//
// Ese error es deliberado en vez de "ejecutar directo igual": una
// escritura contra otro bloque dentro de un BEGIN/COMMIT no puede
// entrar en la transacción (TxManager guarda un solo db+block por
// transacción), y aplicarla al margen sería justo el comportamiento
// silencioso que este archivo viene a corregir -- el operador creería
// que un ROLLBACK posterior la deshace.
func txFor(sess *Session, db, block string) (*Transaction, error) {
	if sess == nil || sess.currentTx == nil || sess.currentTx.Status != "pending" {
		return nil, nil
	}
	tx := sess.currentTx
	if tx.Block == "" {
		return nil, fmt.Errorf("the open transaction %s has no target block; open it as BEGIN <db> <block> to run INSERT/UPDATE/DELETE inside it", tx.ID)
	}
	if tx.DB != db || tx.Block != block {
		return nil, fmt.Errorf("the open transaction %s targets %s.%s; statements between BEGIN and COMMIT must use that same block (got %s.%s)",
			tx.ID, tx.DB, tx.Block, db, block)
	}
	return tx, nil
}

// execInsertDoc inserta un documento suelto. customID vacío significa
// "id autogenerado". fast elige InsertFast (camino rápido con batching
// de la ruta sin id explícito) en lugar de Insert cuando no hay
// transacción; dentro de una transacción no aplica, porque la escritura
// no toca el almacenamiento hasta el COMMIT.
func execInsertDoc(eng *Engine, sess *Session, db, block, customID string, data map[string]any, fast bool) (*Document, error) {
	tx, err := txFor(sess, db, block)
	if err != nil {
		return nil, err
	}
	if tx != nil {
		if customID != "" {
			data["id"] = customID
		}
		return eng.txManager.Insert(tx, data)
	}
	if customID != "" {
		return eng.insertWithID(db, block, customID, data)
	}
	if fast {
		return eng.InsertFast(db, block, data)
	}
	return eng.Insert(db, block, data)
}

// execInsertBatch inserta un lote. Fuera de transacción delega en
// InsertBatch tal cual; dentro, acumula un documento por mutación (el
// COMMIT las aplica juntas de todos modos, en un solo WriteBatch).
func execInsertBatch(eng *Engine, sess *Session, db, block string, docs []map[string]any) (int64, int64, error) {
	tx, err := txFor(sess, db, block)
	if err != nil {
		return 0, 0, err
	}
	if tx != nil {
		var inserted, errors int64
		for _, d := range docs {
			if _, err := eng.txManager.Insert(tx, d); err != nil {
				errors++
			} else {
				inserted++
			}
		}
		return inserted, errors, nil
	}
	inserted, errors := eng.InsertBatch(db, block, docs)
	return inserted, errors, nil
}

func execUpdate(eng *Engine, sess *Session, db, block string, filters []Filter, where *parse.Expr, op UpdateOp) (int64, error) {
	tx, err := txFor(sess, db, block)
	if err != nil {
		return 0, err
	}
	if tx != nil {
		return eng.txManager.Update(tx, filters, where, op)
	}
	return eng.Update(db, block, filters, where, op)
}

func execDelete(eng *Engine, sess *Session, db, block string, filters []Filter, where *parse.Expr) (int64, error) {
	tx, err := txFor(sess, db, block)
	if err != nil {
		return 0, err
	}
	if tx != nil {
		return eng.txManager.Delete(tx, filters, where)
	}
	return eng.Delete(db, block, filters, where)
}
