# Concepts: The Extension API Server

While Views provide a powerful way to manage intermediate state within Δ-controller, they are in-memory and not stored in Kubernetes' main `etcd` database. So how do you inspect, debug, or interact with them? The answer is the **Extension API Server**.

Δ-controller includes an optional, embedded Kubernetes-style API server that runs within the manager. Its sole purpose is to expose your in-memory Views over a standard, Kubernetes-compatible REST API. This makes the internal, "magic" state of your operators transparent and accessible using the tools you already know, like `kubectl` and `curl`.

Some cautionary remarks apply:

*   **Views Only**: The extension API server *only* serves internal view resources. You cannot use it to query native Kubernetes resources like `Pods` or `Services`; those requests will fail. Use a `kubectl` CLI client configured for reaching the main API server to do that.
*   **Read-Write Access**: While primarily used for inspection, the API server supports full CRUD (Create, Read, Update, Delete) operations. This can be a powerful debugging tool, allowing you to manually create or modify a view object to trigger a downstream controller and observe its behavior.
*   **Ephemeral State**: Remember that views are in-memory. If the Δ-controller manager pod restarts, all views will be cleared and then repopulated as the controllers re-reconcile their sources.
*   **Secure by Default**: The API server supports JWT-based authentication and RBAC authorization, allowing fine-grained access control to different users and teams. It can run in development mode (HTTP without authentication) or production mode (HTTPS with authentication).

For instructions on connecting to the Extension API Server, see the [Getting started guide](getting-started.md#accessing-the-extension-api-server). Once connected, you can interact with your views as if they were standard Custom Resources.

## Authentication and Authorization

The extension API server uses JWT (JSON Web Token) based authentication combined with Kubernetes-style RBAC (Role-Based Access Control) to secure access to view resources. This allows you to create fine-grained access policies for different users and teams.

* **JWT Tokens**: Users authenticate using JWT tokens embedded in their kubeconfig files. Tokens are signed using the API server's private key and validated using the corresponding public key (extracted from the TLS certificate).

* **Namespace Restrictions**: Each token can specify which namespaces the user is allowed to access. Users can be restricted to specific namespaces (e.g., `team-a`, `team-b`) or granted wildcard access (`*`) to all namespaces.

* **RBAC Rules**: Tokens include Kubernetes PolicyRules that define which operations (verbs) the user can perform on which API groups and resources. This follows the standard Kubernetes RBAC model.

* **Resource-Level Restrictions**: The `resourceNames` field in PolicyRules allows restricting access to specific named resources. For example, a user might have permission to update `view-1` and `view-2` but not other views. Note that `resourceNames` follows Kubernetes RBAC semantics:
  - **Compatible with**: `get`, `update`, `patch`, `delete` (operations on specific named resources)
  - **Ignored for**: `list`, `watch`, `create`, `deletecollection` (collection-level operations)
  - A rule with `verbs: ["get", "list"]` and `resourceNames: ["view-1"]` will allow listing ALL views but restrict `get` to only `view-1`.

### Authorization Flow

When a request arrives at the API server, the authorization process works as follows:

1. **Namespace Check** (fast path): If the user has namespace restrictions, the server first checks if the requested namespace is allowed. Cross-namespace operations (like cluster-wide LIST or WATCH) are only permitted for users with wildcard (`*`) namespace access.

2. **RBAC Check**: If namespace checks pass, the server evaluates the user's RBAC rules against the requested operation. The rules are checked in the same way as Kubernetes RBAC, matching the verb, API group, and resource type.

3. **Result**: If both checks pass, the request is allowed. Otherwise, it is denied with a `403 Forbidden` status.

### Creating User Credentials

The `dctl` CLI tool provides commands for managing user access:

1. **Generate TLS Certificate and Key**:
   ```bash
   dctl generate-keys
   ```
   This creates `apiserver.key` (private key for signing tokens) and `apiserver.crt` (certificate for TLS and token validation).

2. **Create a Kubeconfig for a User**:
   ```bash
   # Read-only access to default namespace
   dctl generate-config --user=viewer --namespaces=default \
     --rules='[{"verbs":["get","list","watch"],"apiGroups":["*"],"resources":["*"]}]' \
     --tls-key-file=apiserver.key --server-address=localhost:8443 > viewer.config

   # Full access to multiple namespaces
   dctl generate-config --user=developer --namespaces=team-a,team-b \
     --tls-key-file=apiserver.key --server-address=localhost:8443 > developer.config

   # Admin access (all namespaces, all operations)
   dctl generate-config --user=admin --namespaces="*" \
     --tls-key-file=apiserver.key --server-address=localhost:8443 > admin.config
   ```

3. **Use the Kubeconfig**:
   ```bash
   export KUBECONFIG=./viewer.config
   kubectl get myoperator.view.dcontroller.io
   ```

### Examples

#### Scenario 1: Read-Only Access to Specific Namespaces

Create a viewer role with read-only access to views in the `production` namespace:

```bash
cat > viewer-rules.json <<EOF
[
  {
    "verbs": ["get", "list", "watch"],
    "apiGroups": ["*.view.dcontroller.io"],
    "resources": ["*"]
  }
]
EOF

dctl generate-config --user=viewer --namespaces=production \
  --rules-file=viewer-rules.json \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > viewer.config
```

**Allowed operations**:
```bash
export KUBECONFIG=./viewer.config

# ✓ Allowed: GET in production namespace
kubectl get healthview.svc-operator.view.dcontroller.io web-app -n production

# ✓ Allowed: LIST in production namespace
kubectl get healthview.svc-operator.view.dcontroller.io -n production

# ✓ Allowed: WATCH in production namespace
kubectl get healthview.svc-operator.view.dcontroller.io -n production --watch
```

**Denied operations**:
```bash
# ✗ Denied: Cross-namespace LIST (namespace-restricted users cannot list across all namespaces)
kubectl get healthview.svc-operator.view.dcontroller.io --all-namespaces
# Error: access to namespace forbidden: cross-namespace operations not allowed with namespace restrictions

# ✗ Denied: CREATE (not in RBAC rules)
kubectl create -f view.yaml -n production
# Error: forbidden: user not authorized to create healthview in API group svc-operator.view.dcontroller.io

# ✗ Denied: GET in different namespace
kubectl get healthview.svc-operator.view.dcontroller.io test -n staging
# Error: access to namespace staging denied
```

#### Scenario 2: Read-Only Access with Resource Name Restrictions

Create a viewer role that can list all views but only get details for specific views:

```bash
cat > viewer-restricted-rules.json <<EOF
[
  {
    "verbs": ["get", "list", "watch"],
    "apiGroups": ["*.view.dcontroller.io"],
    "resources": ["*"],
    "resourceNames": ["web-app", "api-service"]
  }
]
EOF

dctl generate-config --user=restricted-viewer --namespaces=production \
  --rules-file=viewer-restricted-rules.json \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > restricted-viewer.config
```

**Allowed operations**:
```bash
export KUBECONFIG=./restricted-viewer.config

# ✓ Allowed: LIST in production namespace (resourceNames ignored for list)
kubectl get healthview.svc-operator.view.dcontroller.io -n production

# ✓ Allowed: GET specific allowed resources
kubectl get healthview.svc-operator.view.dcontroller.io web-app -n production
kubectl get healthview.svc-operator.view.dcontroller.io api-service -n production

# ✓ Allowed: WATCH in production namespace (resourceNames ignored for watch)
kubectl get healthview.svc-operator.view.dcontroller.io -n production --watch
```

**Denied operations**:
```bash
# ✗ Denied: GET resources not in resourceNames list
kubectl get healthview.svc-operator.view.dcontroller.io database -n production
# Error: forbidden: user not authorized to get healthview/database

# ✗ Denied: UPDATE (verb not in rule)
kubectl patch healthview.svc-operator.view.dcontroller.io web-app -n production --type=merge -p '{"status":"updated"}'
# Error: forbidden: user not authorized to patch healthview in API group svc-operator.view.dcontroller.io
```

#### Scenario 3: Full Access to Team Namespaces

Create a developer role with full access to multiple team namespaces:

```bash
dctl generate-config --user=developer --namespaces=team-a,team-b,team-c \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > developer.config
```

When `--rules-file` is omitted, the user gets full access (all verbs on all resources).

**Allowed operations**:
```bash
export KUBECONFIG=./developer.config

# ✓ Allowed: Full CRUD in allowed namespaces
kubectl get healthview.svc-operator.view.dcontroller.io -n team-a
kubectl create -f view.yaml -n team-b
kubectl delete healthview.svc-operator.view.dcontroller.io test -n team-c
```

**Denied operations**:
```bash
# ✗ Denied: Cross-namespace LIST (no wildcard access)
kubectl get healthview.svc-operator.view.dcontroller.io --all-namespaces
# Error: cross-namespace operations not allowed with namespace restrictions

# ✗ Denied: Access to namespaces outside allowed list
kubectl get healthview.svc-operator.view.dcontroller.io -n production
# Error: access to namespace production denied
```

#### Scenario 4: Cluster-Wide Admin Access

Create an admin role with unrestricted access:

```bash
dctl generate-config --user=admin --namespaces="*" \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > admin.config
```

**All operations allowed**:
```bash
export KUBECONFIG=./admin.config

# ✓ Allowed: Cross-namespace operations
kubectl get healthview.svc-operator.view.dcontroller.io --all-namespaces

# ✓ Allowed: Full CRUD in any namespace
kubectl get healthview.svc-operator.view.dcontroller.io -n production
kubectl create -f view.yaml -n staging
kubectl delete healthview.svc-operator.view.dcontroller.io test -n team-a
```

### Verifying Token Information

You can inspect the claims embedded in a kubeconfig token:

```bash
export KUBECONFIG=./viewer.config
dctl get-config --tls-cert-file=apiserver.crt
```

Output:
```
👤 User Information:
   Username:   restricted-viewer
   Namespaces: [production]
   Rules: 1 RBAC policy rules
     [1] verbs=[get list watch] apiGroups=[*.view.dcontroller.io] resources=[*] resourceNames=[web-app api-service]

⏱️  Token Metadata:
   Issuer:     dcontroller
   Issued At:  2025-01-15T10:30:00Z
   Expires At: 2026-01-15T10:30:00Z
   Not Before: 2025-01-15T10:30:00Z
✅ Token is VALID
```

## Discovering View API Groups

The first step is to see what view types are available. The API groups for views are generated dynamically from your `Operator` names. You can discover them using `kubectl api-resources`.
```bash
kubectl api-resources
```

The output will show the registered view kinds. Notice the special naming convention for the `APIVERSION`:
```
NAME         SHORTNAMES   APIVERSION                                         NAMESPACED   KIND
healthview                svc-health-operator.view.dcontroller.io/v1alpha1   true         HealthView
```

The API group for a view is always **`<operator-name>.view.dcontroller.io`** and the version is `v1alpha1`. This unique naming prevents conflicts between views defined in different operators.

## Getting and Listing Views

You can use standard `kubectl get` and `kubectl list` commands to inspect your view objects. Because views are schemaless and can have any structure, it's best to use the `-o yaml` or `-o json` output format to see their full content.

Get a specific `HealthView` object from the `svc-health-operator`:

```bash
kubectl get healthview.svc-health-operator.view.dcontroller.io web-app -o yaml
apiVersion: svc-health-operator.view.dcontroller.io/v1alpha1
kind: HealthView
metadata:
  name: web-app
  namespace: default
pods:
- podName: web-app-7bfdf6588c-nvb84
  ready: "True"
- podName: web-app-7bfdf6588c-vkptf
  ready: "True"
```

> **Pro Tip:** Your shell's tab-completion for `kubectl` will work with these long resource names! Type `kubectl get health` and press `Tab` to have the shell complete the full resource name for you.

## Watching Views for Changes

For advanced debugging, you can `watch` a view to see real-time changes as they are processed by your controller pipelines.

The below will set up a watch for all changes to `HealthView` objects:

```bash
kubectl get healthview.svc-health-operator.view.dcontroller.io --watch
```

