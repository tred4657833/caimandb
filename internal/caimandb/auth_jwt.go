package caimandb

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"time"
)

type TokenManager struct {
	secret    []byte
	expiry    time.Duration
	tokens    sync.Map
	blacklist sync.Map
}

type TokenClaims struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	// Database es la base a la que está ligada la identidad (ver
	// User.Database / hierarchy.go). Vacío para ADMIN_GENERAL.
	Database string `json:"database,omitempty"`
	Expires  int64  `json:"expires"`
	IssuedAt int64  `json:"issued_at"`
}

func NewTokenManager(secret string, expiry time.Duration) *TokenManager {
	return &TokenManager{
		secret: []byte(secret),
		expiry: expiry,
	}
}

func (tm *TokenManager) GenerateToken(userID, username, role, database string) (string, error) {
	claims := TokenClaims{
		UserID:   userID,
		Username: username,
		Role:     role,
		Database: database,
		Expires:  time.Now().Add(tm.expiry).Unix(),
		IssuedAt: time.Now().Unix(),
	}

	data, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	h.Write(data)
	h.Write(tm.secret)
	signature := h.Sum(nil)

	token := base64.URLEncoding.EncodeToString(data) + "." + base64.URLEncoding.EncodeToString(signature)
	tm.tokens.Store(token, true)
	return token, nil
}

func (tm *TokenManager) ValidateToken(token string) (*TokenClaims, error) {
	if _, ok := tm.blacklist.Load(token); ok {
		return nil, ErrUnauthorized
	}

	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, ErrUnauthorized
	}

	data, err := base64.URLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}

	sig, err := base64.URLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}

	h := sha256.New()
	h.Write(data)
	h.Write(tm.secret)
	expected := h.Sum(nil)

	if subtle.ConstantTimeCompare(sig, expected) != 1 {
		return nil, ErrUnauthorized
	}

	var claims TokenClaims
	if err := json.Unmarshal(data, &claims); err != nil {
		return nil, err
	}

	if claims.Expires < time.Now().Unix() {
		return nil, ErrUnauthorized
	}

	return &claims, nil
}

func (tm *TokenManager) RevokeToken(token string) {
	tm.blacklist.Store(token, true)
	tm.tokens.Delete(token)
}

func (tm *TokenManager) Cleanup() {
	now := time.Now().Unix()
	tm.tokens.Range(func(key, value interface{}) bool {
		token := key.(string)
		parts := strings.Split(token, ".")
		if len(parts) != 2 {
			tm.tokens.Delete(key)
			return true
		}
		data, err := base64.URLEncoding.DecodeString(parts[0])
		if err != nil {
			tm.tokens.Delete(key)
			return true
		}
		var claims TokenClaims
		if err := json.Unmarshal(data, &claims); err != nil {
			tm.tokens.Delete(key)
			return true
		}
		if claims.Expires < now {
			tm.tokens.Delete(key)
		}
		return true
	})
}
