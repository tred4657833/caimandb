// Package backup implements CaimanDB's encrypted, compressed, incremental
// backup format: JSONL documents, gzip-compressed, then encrypted in
// fixed-size AES-256-GCM chunks so neither side ever has to hold a whole
// backup file in memory. See doc.go for the on-disk layout and manifest.go
// for the full/incremental bookkeeping. The root caimandb package (see
// ops_backup_jsonl.go) is the only caller -- this package has no
// dependency on the Engine, storage pool, or Badger, by design, so it can
// be tested and reasoned about on its own.
package backup

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/crypto/argon2"
)

// KeySize is the AES-256 key size in bytes.
const KeySize = 32

// chunkPlaintextSize bounds how much plaintext is buffered (and how much
// memory Encrypt/Decrypt use) per AES-GCM seal/open call. Backups can be
// arbitrarily large, so streaming in fixed chunks -- rather than sealing
// the whole file in one AEAD call -- keeps memory use bounded regardless
// of database size.
const chunkPlaintextSize = 64 * 1024

// nonceSize is the standard AES-GCM nonce size (96 bits).
const nonceSize = 12

// LoadOrCreateKey resolves the AES-256 key used to encrypt/decrypt every
// backup for one CaimanDB data directory:
//
//   - If the CAIMANDB_BACKUP_KEY environment variable is set, its bytes
//     are stretched into a 32-byte key with Argon2id (the same KDF family
//     already used for password hashing elsewhere in this project -- see
//     users_auth.go), salted with a per-install random value persisted at
//     <dataRoot>/backup/.salt (created on first use, 0600). This is the
//     recommended production path: the passphrase itself is never written
//     to disk, only the salt.
//   - Otherwise, a random 32-byte key is generated on first use and
//     persisted at <dataRoot>/backup/.key (0600), so automatic/scheduled
//     backups work out of the box without any operator setup.
//
// Known limitation, stated plainly rather than glossed over: in the
// no-env-var path, the key lives on the same disk as the data and the
// backups it protects. That still stops someone who copies just the
// backup files (e.g. off a shared drive, an object-storage bucket, a
// stolen external disk) without also getting .key, but it does NOT
// protect against the whole backups directory being read together --
// for that, set CAIMANDB_BACKUP_KEY to a real passphrase kept outside
// the backups directory (a secrets manager, a separate encrypted
// volume, etc.) and back up .salt (not the passphrase) alongside it if
// you ever move the backups directory to a new host.
func LoadOrCreateKey(backupRoot string) ([]byte, error) {
	if err := os.MkdirAll(backupRoot, 0750); err != nil {
		return nil, fmt.Errorf("creating backups directory: %w", err)
	}

	if passphrase := os.Getenv("CAIMANDB_BACKUP_KEY"); passphrase != "" {
		saltPath := filepath.Join(backupRoot, ".salt")
		salt, err := loadOrCreateRandomFile(saltPath, 16)
		if err != nil {
			return nil, fmt.Errorf("loading backup salt: %w", err)
		}
		// Argon2id parameters: 1 pass, 64 MiB, 4 threads -- deliberately
		// modest (not the OWASP-recommended interactive minimum) because
		// this key derivation can run once per scheduled backup cycle,
		// not just once at login; a heavier profile would make frequent
		// automatic backups needlessly slow. Still far stronger than an
		// unsalted hash for this threat model (a copied backup file
		// without the passphrase).
		key := argon2.IDKey([]byte(passphrase), salt, 1, 64*1024, 4, KeySize)
		return key, nil
	}

	keyPath := filepath.Join(backupRoot, ".key")
	return loadOrCreateRandomFile(keyPath, KeySize)
}

// loadOrCreateRandomFile returns the contents of path if it already
// exists and is exactly n bytes, otherwise generates n random bytes,
// persists them to path (0600), and returns those.
func loadOrCreateRandomFile(path string, n int) ([]byte, error) {
	if data, err := os.ReadFile(path); err == nil && len(data) == n {
		return data, nil
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, buf, 0600); err != nil {
		return nil, err
	}
	return buf, nil
}

// EncryptStream reads plaintext from r and writes an encrypted, framed
// stream to w: a repeating sequence of
//
//	[4-byte big-endian ciphertext length][12-byte nonce][ciphertext+tag]
//
// one frame per chunkPlaintextSize (or smaller, for the final chunk) of
// plaintext. Each chunk gets its own random nonce, so nonce reuse -- the
// one catastrophic mistake with AES-GCM -- can't happen across chunks or
// across files sharing the same key.
func EncryptStream(w io.Writer, r io.Reader, key []byte) error {
	aead, err := newAEAD(key)
	if err != nil {
		return err
	}

	buf := make([]byte, chunkPlaintextSize)
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			if err := encryptChunk(w, aead, buf[:n]); err != nil {
				return err
			}
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func encryptChunk(w io.Writer, aead cipher.AEAD, plaintext []byte) error {
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, nil)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(ciphertext)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(nonce); err != nil {
		return err
	}
	_, err := w.Write(ciphertext)
	return err
}

// DecryptStream is EncryptStream's inverse: it reads the framed stream
// from r and writes the recovered plaintext to w. A truncated final
// frame or a failed GCM authentication check (tampering, wrong key,
// bit-rot) is reported as an error immediately rather than emitting
// partial/unauthenticated plaintext.
func DecryptStream(w io.Writer, r io.Reader, key []byte) error {
	aead, err := newAEAD(key)
	if err != nil {
		return err
	}

	var lenBuf [4]byte
	for {
		_, err := io.ReadFull(r, lenBuf[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading frame length: %w", err)
		}
		ctLen := binary.BigEndian.Uint32(lenBuf[:])

		nonce := make([]byte, nonceSize)
		if _, err := io.ReadFull(r, nonce); err != nil {
			return fmt.Errorf("reading frame nonce: %w", err)
		}
		ciphertext := make([]byte, ctLen)
		if _, err := io.ReadFull(r, ciphertext); err != nil {
			return fmt.Errorf("reading frame ciphertext: %w", err)
		}
		plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
		if err != nil {
			return fmt.Errorf("decrypting backup chunk (wrong key, or the file is corrupted/tampered): %w", err)
		}
		if _, err := w.Write(plaintext); err != nil {
			return err
		}
	}
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("backup key must be %d bytes, got %d", KeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// FileChecksum computes a SHA-256 hash of a file's contents, used by
// VerifyBackup (see ops_backup_jsonl.go in the root package) to detect
// silent on-disk corruption of a backup file independent of whether it
// can still be decrypted (a flipped bit inside a GCM tag fails to
// decrypt; a flipped bit that happens to land after truncation of the
// last frame might not, depending on where -- the checksum catches
// either case against the value recorded in the manifest at backup
// time).
func FileChecksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
