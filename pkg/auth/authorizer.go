package auth

import (
	"context"

	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// CompositeAuthorizer performs both RBAC checks and namespace-based authorization.
// It validates that users have the required RBAC permissions for the requested operation
// and enforces namespace restrictions based on token claims.
type CompositeAuthorizer struct{}

// NewCompositeAuthorizer creates a new composite authorizer that performs both
// RBAC checks and namespace filtering.
func NewCompositeAuthorizer() *CompositeAuthorizer {
	return &CompositeAuthorizer{}
}

// Authorize implements authorizer.Authorizer. It performs two checks:
// 1. Namespace filtering - enforces namespace restrictions from token claims (fast path)
// 2. RBAC validation - ensures the user has the required permissions (verb, resource, apiGroup)
func (a *CompositeAuthorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
	// Get user info
	user := attr.GetUser()
	if user == nil {
		return authorizer.DecisionDeny, "no user info", nil
	}

	// Only authorize resource requests
	if !attr.IsResourceRequest() {
		// Allow non-resource requests (API discovery, /healthz, etc.)
		return authorizer.DecisionAllow, "", nil
	}

	// Extract request details
	verb := attr.GetVerb()
	apiGroup := attr.GetAPIGroup()
	resource := attr.GetResource()
	name := attr.GetName()
	requestedNamespace := attr.GetNamespace()

	// STEP 1: Check namespace restrictions (fast path - check before expensive RBAC matching)
	allowedNamespaces := user.GetExtra()["namespaces"]
	if len(allowedNamespaces) > 0 {
		// User has namespace restrictions

		// Check if user has wildcard access
		hasWildcard := false
		for _, ns := range allowedNamespaces {
			if ns == "*" {
				hasWildcard = true
				break
			}
		}

		if requestedNamespace == "" {
			// Cross-namespace operation (LIST/WATCH all namespaces) or cluster-scoped resource
			// Only allow if user has wildcard namespace access
			if !hasWildcard {
				return authorizer.DecisionDeny,
					"cross-namespace operations not allowed with namespace restrictions; please specify a namespace", nil
			}
			// User has wildcard access - allow cross-namespace operations
		} else {
			// Specific namespace requested - check if user has access
			if !hasWildcard {
				// Check if user has access to the requested namespace
				hasAccess := false
				for _, ns := range allowedNamespaces {
					if ns == requestedNamespace {
						hasAccess = true
						break
					}
				}
				if !hasAccess {
					return authorizer.DecisionDeny,
						"access to namespace " + requestedNamespace + " denied", nil
				}
			}
		}
	}

	// STEP 2: Check RBAC permissions
	if !CheckRBACAccess(user, verb, apiGroup, resource, name) {
		return authorizer.DecisionDeny,
			"user not authorized to " + verb + " " + resource + " in API group " + apiGroup, nil
	}

	return authorizer.DecisionAllow, "", nil
}
