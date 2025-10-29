package auth

import (
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// JWTAuthenticator validates JWT tokens and extracts user info
type JWTAuthenticator struct {
	publicKey *rsa.PublicKey
}

// Claims represents the JWT claims we use
type Claims struct {
	Username   string              `json:"username"`
	Namespaces []string            `json:"namespaces,omitempty"` // Allowed namespaces (empty = all)
	Rules      []rbacv1.PolicyRule `json:"rules,omitempty"`      // RBAC policy rules (empty = full access)
	jwt.RegisteredClaims
}

// NewJWTAuthenticator creates a new JWT authenticator
func NewJWTAuthenticator(publicKey *rsa.PublicKey) *JWTAuthenticator {
	return &JWTAuthenticator{publicKey: publicKey}
}

// AuthenticateRequest implements authenticator.Request
func (a *JWTAuthenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	// Extract bearer token
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return nil, false, nil // No auth provided
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, false, fmt.Errorf("invalid authorization header format")
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")

	// Parse and validate JWT
	claims := &Claims{}
	jwtToken, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.publicKey, nil
	})

	if err != nil || !jwtToken.Valid {
		return nil, false, fmt.Errorf("invalid token: %w", err)
	}

	// Build user info with namespace and RBAC rules
	extra := make(map[string][]string)
	if len(claims.Namespaces) > 0 {
		extra["namespaces"] = claims.Namespaces
	}

	// Serialize rules to JSON and store in extra
	if len(claims.Rules) > 0 {
		rulesJSON, err := json.Marshal(claims.Rules)
		if err != nil {
			return nil, false, fmt.Errorf("failed to serialize rules: %w", err)
		}
		extra["rules"] = []string{string(rulesJSON)}
	}

	userInfo := &user.DefaultInfo{
		Name:  claims.Username,
		Extra: extra,
	}

	return &authenticator.Response{
		User: userInfo,
	}, true, nil
}
