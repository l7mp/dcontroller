package auth_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dcontroller/pkg/auth"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apiserver/pkg/authentication/user"
)

var _ = Describe("RBAC Access Control", func() {
	var userInfo user.Info

	Context("CheckRBACAccess with empty rules", func() {
		BeforeEach(func() {
			// User with no rules = full access
			userInfo = &user.DefaultInfo{
				Name:  "test-user",
				Extra: map[string][]string{},
			}
		})

		It("should allow any operation when rules are empty", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "list", "apps", "deployments", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "delete", "batch", "jobs", "my-job")).To(BeTrue())
		})
	})

	Context("CheckRBACAccess with specific rules", func() {
		BeforeEach(func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get", "list", "watch"},
					APIGroups: []string{""},
					Resources: []string{"pods", "services"},
				},
				{
					Verbs:     []string{"get", "list", "create", "update", "delete"},
					APIGroups: []string{"myoperator.view.dcontroller.io"},
					Resources: []string{"*"},
				},
			}

			rulesJSON := mustMarshalJSON(rules)
			userInfo = &user.DefaultInfo{
				Name:  "alice",
				Extra: map[string][]string{"rules": {rulesJSON}},
			}
		})

		It("should allow get pods in core API group", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "")).To(BeTrue())
		})

		It("should allow list services in core API group", func() {
			Expect(auth.CheckRBACAccess(userInfo, "list", "", "services", "")).To(BeTrue())
		})

		It("should allow watch services in core API group", func() {
			Expect(auth.CheckRBACAccess(userInfo, "watch", "", "services", "")).To(BeTrue())
		})

		It("should deny create pods (not in verbs)", func() {
			Expect(auth.CheckRBACAccess(userInfo, "create", "", "pods", "")).To(BeFalse())
		})

		It("should deny get configmaps (not in resources)", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "configmaps", "")).To(BeFalse())
		})

		It("should deny get pods in apps group (wrong API group)", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "apps", "pods", "")).To(BeFalse())
		})

		It("should allow all operations on custom API group resources", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "myoperator.view.dcontroller.io", "anyresource", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "create", "myoperator.view.dcontroller.io", "anyresource", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "delete", "myoperator.view.dcontroller.io", "anotherresource", "")).To(BeTrue())
		})
	})

	Context("CheckRBACAccess with wildcards", func() {
		BeforeEach(func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"*"},
					APIGroups: []string{"*"},
					Resources: []string{"*"},
				},
			}

			rulesJSON := mustMarshalJSON(rules)
			userInfo = &user.DefaultInfo{
				Name:  "admin",
				Extra: map[string][]string{"rules": {rulesJSON}},
			}
		})

		It("should allow any operation on any resource", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "delete", "apps", "deployments", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "create", "custom.io", "widgets", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "patch", "batch", "cronjobs", "my-job")).To(BeTrue())
		})
	})

	Context("CheckRBACAccess with resource names", func() {
		BeforeEach(func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:         []string{"get", "update"},
					APIGroups:     []string{""},
					Resources:     []string{"pods"},
					ResourceNames: []string{"pod-1", "pod-2"},
				},
			}

			rulesJSON := mustMarshalJSON(rules)
			userInfo = &user.DefaultInfo{
				Name:  "bob",
				Extra: map[string][]string{"rules": {rulesJSON}},
			}
		})

		It("should allow get on specific pod names", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "pod-1")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "pod-2")).To(BeTrue())
		})

		It("should deny get on other pod names", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "pod-3")).To(BeFalse())
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "other-pod")).To(BeFalse())
		})

		It("should allow list without specifying name (empty resourceNames = all allowed)", func() {
			// When checking list operations, we typically don't provide a specific name
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "")).To(BeTrue())
		})
	})

	Context("CheckRBACAccess with multiple rules (OR logic)", func() {
		BeforeEach(func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get"},
					APIGroups: []string{""},
					Resources: []string{"pods"},
				},
				{
					Verbs:     []string{"list"},
					APIGroups: []string{""},
					Resources: []string{"services"},
				},
			}

			rulesJSON := mustMarshalJSON(rules)
			userInfo = &user.DefaultInfo{
				Name:  "charlie",
				Extra: map[string][]string{"rules": {rulesJSON}},
			}
		})

		It("should allow operations matching first rule", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "")).To(BeTrue())
		})

		It("should allow operations matching second rule", func() {
			Expect(auth.CheckRBACAccess(userInfo, "list", "", "services", "")).To(BeTrue())
		})

		It("should deny operations not matching any rule", func() {
			Expect(auth.CheckRBACAccess(userInfo, "list", "", "pods", "")).To(BeFalse())
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "services", "")).To(BeFalse())
			Expect(auth.CheckRBACAccess(userInfo, "delete", "", "pods", "")).To(BeFalse())
		})
	})

	Context("CheckRBACAccess with API group variations", func() {
		BeforeEach(func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get", "list"},
					APIGroups: []string{"", "apps", "batch"},
					Resources: []string{"*"},
				},
			}

			rulesJSON := mustMarshalJSON(rules)
			userInfo = &user.DefaultInfo{
				Name:  "developer",
				Extra: map[string][]string{"rules": {rulesJSON}},
			}
		})

		It("should allow access to core API group (empty string)", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "list", "", "services", "")).To(BeTrue())
		})

		It("should allow access to apps API group", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "apps", "deployments", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "list", "apps", "statefulsets", "")).To(BeTrue())
		})

		It("should allow access to batch API group", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "batch", "jobs", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "list", "batch", "cronjobs", "")).To(BeTrue())
		})

		It("should deny access to networking API group", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "networking.k8s.io", "ingresses", "")).To(BeFalse())
		})
	})

	Context("CheckRBACAccess with malformed rules JSON", func() {
		BeforeEach(func() {
			userInfo = &user.DefaultInfo{
				Name:  "malformed-user",
				Extra: map[string][]string{"rules": {"invalid json"}},
			}
		})

		It("should deny access when rules cannot be deserialized", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "", "pods", "")).To(BeFalse())
		})
	})

	Context("CheckRBACAccess with wildcard patterns in APIGroups", func() {
		BeforeEach(func() {
			rules := []rbacv1.PolicyRule{
				{
					Verbs:     []string{"get", "list", "watch"},
					APIGroups: []string{"*.view.dcontroller.io"},
					Resources: []string{"*"},
				},
				{
					Verbs:     []string{"get"},
					APIGroups: []string{"kube-*"},
					Resources: []string{"pods"},
				},
			}

			rulesJSON := mustMarshalJSON(rules)
			userInfo = &user.DefaultInfo{
				Name:  "wildcard-user",
				Extra: map[string][]string{"rules": {rulesJSON}},
			}
		})

		It("should match prefix wildcard *.view.dcontroller.io", func() {
			// Should match any API group ending with .view.dcontroller.io
			Expect(auth.CheckRBACAccess(userInfo, "get", "myoperator.view.dcontroller.io", "healthview", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "list", "svc-health-operator.view.dcontroller.io", "healthview", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "watch", "foo.bar.view.dcontroller.io", "anything", "")).To(BeTrue())
		})

		It("should not match API groups that don't end with .view.dcontroller.io", func() {
			Expect(auth.CheckRBACAccess(userInfo, "get", "apps", "deployments", "")).To(BeFalse())
			Expect(auth.CheckRBACAccess(userInfo, "get", "view.dcontroller.io.something", "pods", "")).To(BeFalse())
		})

		It("should match suffix wildcard kube-*", func() {
			// Should match any API group starting with kube-
			Expect(auth.CheckRBACAccess(userInfo, "get", "kube-system", "pods", "")).To(BeTrue())
			Expect(auth.CheckRBACAccess(userInfo, "get", "kube-public", "pods", "")).To(BeTrue())
		})

		It("should not match verbs not in the rule", func() {
			Expect(auth.CheckRBACAccess(userInfo, "create", "myoperator.view.dcontroller.io", "healthview", "")).To(BeFalse())
			Expect(auth.CheckRBACAccess(userInfo, "delete", "svc-health-operator.view.dcontroller.io", "healthview", "")).To(BeFalse())
		})
	})
})
