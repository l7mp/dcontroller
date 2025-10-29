package auth_test

import (
	"crypto/rsa"
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/golang-jwt/jwt/v5"
	"github.com/l7mp/dcontroller/pkg/auth"
	rbacv1 "k8s.io/api/rbac/v1"
)

func TestAuth(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Auth Package Suite")
}

// Helper functions

func mustGenerateKeyPair() (*rsa.PrivateKey, *rsa.PublicKey) {
	cert, key, err := auth.GenerateSelfSignedCert("localhost")
	Expect(err).NotTo(HaveOccurred())
	privateKey, err := auth.ParsePrivateKey(key)
	Expect(err).NotTo(HaveOccurred())
	publicKey, err := auth.ParsePublicKey(cert)
	Expect(err).NotTo(HaveOccurred())
	return privateKey, publicKey
}

func mustMarshalJSON(rules []rbacv1.PolicyRule) string {
	data, err := json.Marshal(rules)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return string(data)
}

func mustParseToken(tokenString string, publicKey *rsa.PublicKey) *auth.Claims {
	claims := &auth.Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		return publicKey, nil
	})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, token.Valid).To(BeTrue())
	return claims
}
