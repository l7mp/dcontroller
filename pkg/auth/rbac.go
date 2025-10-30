package auth

import (
	"encoding/json"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apiserver/pkg/authentication/user"
)

// CheckRBACAccess checks if the user has permission to perform the requested action
// Returns true if any of the user's RBAC rules allow the action
func CheckRBACAccess(userInfo user.Info, verb, apiGroup, resource, resourceName string) bool {
	// Extract rules from user.Extra
	rulesJSON := userInfo.GetExtra()["rules"]
	if len(rulesJSON) == 0 {
		// No rules defined = full access (backward compat / admin mode)
		return true
	}

	// Deserialize rules
	var rules []rbacv1.PolicyRule
	if err := json.Unmarshal([]byte(rulesJSON[0]), &rules); err != nil {
		// If deserialization fails, deny access
		return false
	}

	// Check if any rule matches
	for _, rule := range rules {
		if ruleMatches(rule, verb, apiGroup, resource, resourceName) {
			return true
		}
	}

	return false
}

// ruleMatches checks if a single PolicyRule matches the requested action
func ruleMatches(rule rbacv1.PolicyRule, verb, apiGroup, resource, resourceName string) bool {
	// Check verb
	if !matches(rule.Verbs, verb) {
		return false
	}

	// Check API group
	if !matches(rule.APIGroups, apiGroup) {
		return false
	}

	// Check resource
	if !matches(rule.Resources, resource) {
		return false
	}

	// Check resource name (if specified in rule)
	// Empty ResourceNames means all names are allowed
	if len(rule.ResourceNames) > 0 && resourceName != "" {
		if !matches(rule.ResourceNames, resourceName) {
			return false
		}
	}

	return true
}

// matches checks if the requested value is in the rule list
// Supports:
// - Exact match: "core" matches "core"
// - Full wildcard: "*" matches anything
// - Prefix wildcard: "*.example.com" matches "foo.example.com", "bar.example.com", etc.
// - Suffix wildcard: "kube-*" matches "kube-system", "kube-public", etc.
func matches(ruleValues []string, requested string) bool {
	for _, ruleValue := range ruleValues {
		// Full wildcard
		if ruleValue == "*" {
			return true
		}

		// Exact match
		if ruleValue == requested {
			return true
		}

		// Wildcard pattern matching
		if strings.Contains(ruleValue, "*") {
			if matchesWildcard(ruleValue, requested) {
				return true
			}
		}
	}
	return false
}

// matchesWildcard checks if a value matches a wildcard pattern
// Supports "*" at the beginning or end of the pattern
func matchesWildcard(pattern, value string) bool {
	// Handle prefix wildcard: "*.example.com"
	if strings.HasPrefix(pattern, "*") {
		suffix := strings.TrimPrefix(pattern, "*")
		return strings.HasSuffix(value, suffix)
	}

	// Handle suffix wildcard: "kube-*"
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(value, prefix)
	}

	// Handle middle wildcard: "foo*bar" (split on first *)
	parts := strings.SplitN(pattern, "*", 2)
	if len(parts) == 2 {
		return strings.HasPrefix(value, parts[0]) && strings.HasSuffix(value, parts[1])
	}

	return false
}
