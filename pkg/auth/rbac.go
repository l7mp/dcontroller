package auth

import (
	"encoding/json"

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
// Supports wildcard "*" matching
func matches(ruleValues []string, requested string) bool {
	for _, ruleValue := range ruleValues {
		if ruleValue == "*" {
			return true
		}
		if ruleValue == requested {
			return true
		}
	}
	return false
}
