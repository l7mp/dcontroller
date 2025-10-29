package auth_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dcontroller/pkg/auth"
	rbacv1 "k8s.io/api/rbac/v1"
)

var _ = Describe("Token Generation and Parsing", func() {
	var (
		privateKey, publicKey = mustGenerateKeyPair()
		generator             = auth.NewTokenGenerator(privateKey)
	)

	Context("Token generation with RBAC rules", func() {
		It("should generate valid token with simple rules", func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get", "list"},
					APIGroups: []string{""},
					Resources: []string{"pods"},
				},
			}

			token, err := generator.GenerateToken("alice", []string{"team-a"}, rules, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())
		})

		It("should generate valid token with multiple rules", func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get", "list", "watch"},
					APIGroups: []string{""},
					Resources: []string{"pods", "services"},
				},
				{
					Verbs:     []string{"*"},
					APIGroups: []string{"myoperator.view.dcontroller.io"},
					Resources: []string{"*"},
				},
			}

			token, err := generator.GenerateToken("bob", []string{"team-b", "shared"}, rules, 24*time.Hour)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())
		})

		It("should generate valid token with no rules (full access)", func() {
			token, err := generator.GenerateToken("admin", []string{"*"}, nil, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())
		})

		It("should generate valid token with resource names", func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:         []string{"get", "update"},
					APIGroups:     []string{""},
					Resources:     []string{"pods"},
					ResourceNames: []string{"pod-1", "pod-2"},
				},
			}

			token, err := generator.GenerateToken("charlie", []string{"dev"}, rules, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())
		})
	})

	Context("Token authentication and claims extraction", func() {
		It("should extract claims from token with rules", func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get", "list"},
					APIGroups: []string{""},
					Resources: []string{"pods"},
				},
			}

			token, err := generator.GenerateToken("alice", []string{"team-a"}, rules, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())

			// Parse the token using the authenticator
			// Note: The authenticator expects an HTTP request, so we'll test the Claims directly
			claims := mustParseToken(token, publicKey)

			Expect(claims.Username).To(Equal("alice"))
			Expect(claims.Namespaces).To(Equal([]string{"team-a"}))
			Expect(claims.Rules).To(HaveLen(1))
			Expect(claims.Rules[0].Verbs).To(ConsistOf("get", "list"))
			Expect(claims.Rules[0].APIGroups).To(ConsistOf(""))
			Expect(claims.Rules[0].Resources).To(ConsistOf("pods"))
		})

		It("should extract empty namespaces and rules", func() {
			token, err := generator.GenerateToken("guest", nil, nil, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())

			claims := mustParseToken(token, publicKey)

			Expect(claims.Username).To(Equal("guest"))
			Expect(claims.Namespaces).To(BeEmpty())
			Expect(claims.Rules).To(BeEmpty())
		})

		It("should extract multiple rules correctly", func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get"},
					APIGroups: []string{""},
					Resources: []string{"pods"},
				},
				{
					Verbs:     []string{"list"},
					APIGroups: []string{"apps"},
					Resources: []string{"deployments"},
				},
			}

			token, err := generator.GenerateToken("developer", []string{"dev", "staging"}, rules, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())

			claims := mustParseToken(token, publicKey)

			Expect(claims.Username).To(Equal("developer"))
			Expect(claims.Namespaces).To(ConsistOf("dev", "staging"))
			Expect(claims.Rules).To(HaveLen(2))
		})

		It("should handle wildcard rules", func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"*"},
					APIGroups: []string{"*"},
					Resources: []string{"*"},
				},
			}

			token, err := generator.GenerateToken("admin", []string{"*"}, rules, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())

			claims := mustParseToken(token, publicKey)

			Expect(claims.Username).To(Equal("admin"))
			Expect(claims.Namespaces).To(Equal([]string{"*"}))
			Expect(claims.Rules).To(HaveLen(1))
			Expect(claims.Rules[0].Verbs).To(ConsistOf("*"))
			Expect(claims.Rules[0].APIGroups).To(ConsistOf("*"))
			Expect(claims.Rules[0].Resources).To(ConsistOf("*"))
		})
	})

	Context("Token expiry", func() {
		It("should respect expiry time", func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get"},
					APIGroups: []string{""},
					Resources: []string{"pods"},
				},
			}

			token, err := generator.GenerateToken("alice", []string{"team-a"}, rules, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())

			claims := mustParseToken(token, publicKey)

			// Check that expiry is set to approximately 1 hour from now
			expiryTime := claims.ExpiresAt.Time
			expectedExpiry := time.Now().Add(1 * time.Hour)
			Expect(expiryTime).To(BeTemporally("~", expectedExpiry, 5*time.Second))
		})
	})

	Context("Token issuer", func() {
		It("should set issuer to 'dcontroller'", func() {
			token, err := generator.GenerateToken("alice", []string{"team-a"}, nil, 1*time.Hour)
			Expect(err).NotTo(HaveOccurred())

			claims := mustParseToken(token, publicKey)

			Expect(claims.Issuer).To(Equal("dcontroller"))
		})
	})
})
