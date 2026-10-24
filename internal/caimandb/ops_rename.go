//
// Shared rename machinery for RENAME DATABASE and RENAME BLOCK. See
// docs/rename-fix-2026-07.md for the bug this replaced and why.
package caimandb

import (
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// markRenaming/unmarkRenaming/isRenaming implement a lightweight registry
// of databases and blocks currently mid-rename. Background jobs that open
// Badger handles or mutate files outside the lockManager's per-operation
// locks -- the maintenance sweep's integrity checks and cold-block
// compaction chief among them -- consult this registry so they skip
// anything mid-rename instead of racing it: reopening a handle right
// after renameDirRobust force-closed it (defeating the close and making
// the OS rename fail with "Access is denied"), or running value-log GC
// concurrently with the copy fallback's raw file walk (changing the file
// count mid-copy, which is exactly what made the fallback's verification
// fail with a source/destination file-count mismatch).
//
// A plain map + RWMutex is enough here: this is a coarse "don't touch
// this right now" signal for cooperative background jobs, not a mutual-
// exclusion primitive guarding the rename itself (renameDirRobust and the
// lockManager already do that for the rename operation's own steps).
func (e *Engine) markRenaming(scope string) {
	e.renamingMu.Lock()
	if e.renaming == nil {
		e.renaming = make(map[string]struct{})
	}
	e.renaming[scope] = struct{}{}
	e.renamingMu.Unlock()
}

func (e *Engine) unmarkRenaming(scope string) {
	e.renamingMu.Lock()
	delete(e.renaming, scope)
	e.renamingMu.Unlock()
}

func (e *Engine) isRenaming(scope string) bool {
	e.renamingMu.RLock()
	_, ok := e.renaming[scope]
	e.renamingMu.RUnlock()
	return ok
}

// isDBOrBlockRenaming reports whether dbName (RENAME DATABASE) or the
// specific dbName/block pair (RENAME BLOCK) currently has a rename in
// flight. block may be "" to check only at database granularity.
func (e *Engine) isDBOrBlockRenaming(dbName, block string) bool {
	if e.isRenaming(dbName) {
		return true
	}
	if block != "" && e.isRenaming(dbName+"\x00"+block) {
		return true
	}
	return false
}

// renameDirRobust renames a CaimanDB-managed directory (a database root
// or a single block) from oldPath to newPath. It is safe to call while
// the directory being renamed is "en arranque" -- i.e. still has live
// Badger handles open somewhere underneath it -- because it force-closes
// every such handle first, so the OS-level rename is never blocked by a
// file CaimanDB itself still has open. It never deletes oldPath until
// the data is confirmed present at newPath, so a failed rename leaves
// the original directory untouched.
func (e *Engine) renameDirRobust(oldPath, newPath string) error {
	closed := e.pool.CloseAndRemoveForceByPrefix(oldPath)
	if len(closed) > 0 {
		log().Info("Closed open handles before rename",
			zap.String("path", oldPath),
			zap.Int("handles", len(closed)))
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	if err := e.waitForFileRelease(oldPath, 10*time.Second); err != nil {
		log().Warn("Timeout waiting for file release before rename",
			zap.String("path", oldPath), zap.Error(err))
	}

	var renameErr error
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * 300 * time.Millisecond
			if delay > 3*time.Second {
				delay = 3 * time.Second
			}
			log().Info("Retrying directory rename",
				zap.String("old", oldPath),
				zap.String("new", newPath),
				zap.Int("attempt", attempt+1))
			time.Sleep(delay)
			runtime.GC()
		}

		renameErr = os.Rename(oldPath, newPath)
		if renameErr == nil {
			return nil
		}

		if !isTransientRenameErr(renameErr) {
			// Not a "file still held open" style error (destination
			// already exists, permission genuinely denied, disk full,
			// etc). Retrying identically won't help, so stop early and
			// go straight to reporting/copy-fallback below.
			break
		}
	}

	log().Warn("Directory rename failed after retries, falling back to copy+verify+delete",
		zap.String("old", oldPath),
		zap.String("new", newPath),
		zap.Error(renameErr))

	const maxCopyAttempts = 3
	var copyErr error
	for copyAttempt := 0; copyAttempt < maxCopyAttempts; copyAttempt++ {
		if copyAttempt > 0 {
			log().Warn("Retrying copy fallback after verification mismatch",
				zap.String("old", oldPath),
				zap.String("new", newPath),
				zap.Int("attempt", copyAttempt+1),
				zap.Error(copyErr))
			// Clear out whatever the previous attempt left at newPath so
			// the retry starts from a clean destination instead of
			// re-verifying (or worse, layering copies on top of) a
			// partial one.
			_ = os.RemoveAll(newPath)
			time.Sleep(500 * time.Millisecond)
		}

		if copyErr = e.copyDatabaseFilesComplete(oldPath, newPath); copyErr != nil {
			continue
		}

		if copyErr = verifyDirCopy(oldPath, newPath); copyErr != nil {
			continue
		}

		if err := os.RemoveAll(oldPath); err != nil {
			log().Warn("Copy fallback succeeded but old directory could not be removed",
				zap.String("path", oldPath),
				zap.Error(err))
		}

		return nil
	}

	// Every copy+verify attempt failed: don't leave a partial/mismatched
	// directory sitting at newPath -- that would look like a second,
	// broken database got created under the new name instead of the
	// rename simply not having happened. oldPath is untouched throughout
	// this whole fallback, so the original database is never at risk;
	// this cleanup only ever removes newPath, which never held anything
	// but an incomplete copy.
	if err := os.RemoveAll(newPath); err != nil {
		log().Warn("Failed to clean up incomplete copy-fallback destination",
			zap.String("path", newPath),
			zap.Error(err))
	}

	return fmt.Errorf("rename failed (%v) and copy fallback did not verify after %d attempts: %w", renameErr, maxCopyAttempts, copyErr)
}

// isTransientRenameErr reports whether err looks like the target is
// still held open by a process (this one or another) rather than a
// real, permanent failure. os.IsPermission is checked first since it's
// OS/locale-independent; the substring list is a fallback for error
// text os.IsPermission doesn't classify as a permission error (e.g.
// Windows' ERROR_SHARING_VIOLATION), and intentionally includes both
// the English and Spanish Windows wording since FormatMessage returns
// localized text.
func isTransientRenameErr(err error) bool {
	if err == nil {
		return false
	}
	if os.IsPermission(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"access is denied",           // Windows, English
		"acceso denegado",            // Windows, Spanish
		"used by another process",    // Windows, English
		"lo est\u00e1 utilizando otro proceso", // Windows, Spanish
		"sharing violation",
		"resource busy",
		"device or resource busy",
		"text file busy",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// verifyDirCopy is a cheap sanity check for the copy fallback: it makes
// sure the destination has at least as many files as the source before
// the source is deleted, so a partial/failed copy can never result in
// silent data loss. It intentionally doesn't compare byte-for-byte
// (that would be expensive for large databases); copyFileWithRetry
// already fsyncs each file and returns an error on any short copy.
func verifyDirCopy(src, dst string) error {
	srcCount, err := countFiles(src)
	if err != nil {
		return fmt.Errorf("could not verify source %s: %w", src, err)
	}
	dstCount, err := countFiles(dst)
	if err != nil {
		return fmt.Errorf("could not verify destination %s: %w", dst, err)
	}
	if dstCount < srcCount {
		return fmt.Errorf("copy incomplete: source has %d file(s), destination has %d", srcCount, dstCount)
	}
	return nil
}

func countFiles(root string) (int, error) {
	n := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			n++
		}
		return nil
	})
	return n, err
}
