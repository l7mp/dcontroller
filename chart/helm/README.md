# Δ-controller Helm Chart

This Helm chart deploys the Δ-controller operator with its embedded API server for managing view resources.

## Installation

### Development Mode (Default)

For development and testing, install with default settings (HTTP mode, no authentication):

```bash
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm install dcontroller dcontroller/dcontroller
```

This deploys:
- API server in HTTP mode (no TLS)
- Authentication disabled
- ClusterIP service (access via port-forward only)

Access the API server:

```bash
kubectl -n dcontroller-system port-forward deployment/dcontroller-manager 8443:8443 &
dctl generate-config --http --insecure --user=dev --namespaces="*" \
  --server-address=localhost:8443 > /tmp/dcontroller-dev.config
export KUBECONFIG=/tmp/dcontroller-dev.config
kubectl api-resources
```

### Production Mode with LoadBalancer

For production deployment with HTTPS and JWT authentication:

1. **Generate TLS certificate and key:**

   ```bash
   dctl generate-keys --hostname=dcontroller-api.example.com
   ```

2. **Create TLS secret:**

   ```bash
   kubectl create namespace dcontroller-system
   kubectl create secret tls dcontroller-tls \
     --cert=apiserver.crt \
     --key=apiserver.key \
     -n dcontroller-system
   ```

3. **Install with production mode:**

   ```bash
   helm install dcontroller dcontroller/dcontroller \
     --set apiServer.mode=production \
     --set apiServer.service.type=LoadBalancer
   ```

4. **Get the LoadBalancer IP/hostname:**

   ```bash
   kubectl get svc dcontroller-apiserver -n dcontroller-system
   ```

5. **Generate user kubeconfig:**

   ```bash
   # For admin access
   dctl generate-config --user=admin --namespaces="*" \
     --tls-key-file=apiserver.key \
     --server-address=<LOADBALANCER-IP>:8443 > admin.config

   # For limited access
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
     --tls-key-file=apiserver.key \
     --server-address=<LOADBALANCER-IP>:8443 > viewer.config
   ```

### Production Mode with NodePort

For on-premises or when LoadBalancer is not available:

```bash
helm install dcontroller dcontroller/dcontroller \
  --set apiServer.mode=production \
  --set apiServer.service.type=NodePort \
  --set apiServer.service.nodePort=30443
```

Access using `<NODE-IP>:30443`.

### Production Mode with Gateway API

For clusters with Gateway API installed:

```bash
helm install dcontroller dcontroller/dcontroller \
  --set apiServer.mode=production \
  --set apiServer.gateway.enabled=true \
  --set apiServer.gateway.gatewayName=my-gateway \
  --set apiServer.gateway.gatewayNamespace=gateway-system \
  --set apiServer.gateway.hostname=dcontroller-api.example.com
```

## Configuration

### Values

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.name` | Docker image name | `docker.io/l7mp/dcontroller` |
| `image.tag` | Docker image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `Always` |
| `apiServer.enabled` | Enable/disable API server | `true` |
| `apiServer.mode` | Deployment mode: `development` or `production` | `development` |
| `apiServer.port` | API server port | `8443` |
| `apiServer.tls.enabled` | Enable TLS (production mode only) | `true` |
| `apiServer.tls.secretName` | Name of TLS secret | `dcontroller-tls` |
| `apiServer.tls.cert` | TLS certificate (inline, base64) | `""` |
| `apiServer.tls.key` | TLS private key (inline, base64) | `""` |
| `apiServer.service.type` | Service type: `ClusterIP`, `LoadBalancer`, or `NodePort` | `ClusterIP` |
| `apiServer.service.nodePort` | NodePort (when type is NodePort) | `nil` |
| `apiServer.service.annotations` | Service annotations | `{}` |
| `apiServer.gateway.enabled` | Enable Gateway API HTTPRoute | `false` |
| `apiServer.gateway.gatewayName` | Gateway name | `""` |
| `apiServer.gateway.gatewayNamespace` | Gateway namespace | `""` |
| `apiServer.gateway.hostname` | HTTPRoute hostname | `dcontroller-api.example.com` |

### Examples

#### Disable API Server Completely

```bash
helm install dcontroller dcontroller/dcontroller \
  --set apiServer.enabled=false
```

#### Provide TLS Certificate Inline (Not Recommended for Production)

```bash
helm install dcontroller dcontroller/dcontroller \
  --set apiServer.mode=production \
  --set-file apiServer.tls.cert=apiserver.crt \
  --set-file apiServer.tls.key=apiserver.key
```

#### AWS LoadBalancer with Annotations

```bash
helm install dcontroller dcontroller/dcontroller \
  --set apiServer.mode=production \
  --set apiServer.service.type=LoadBalancer \
  --set apiServer.service.annotations."service\.beta\.kubernetes\.io/aws-load-balancer-type"="nlb" \
  --set apiServer.service.annotations."service\.beta\.kubernetes\.io/aws-load-balancer-scheme"="internal"
```

## Security Considerations

1. **TLS Certificates**: Always use properly signed certificates in production. Self-signed certificates should only be used for development.

2. **Secret Management**: Never commit TLS certificates or keys to version control. Use Kubernetes secrets or external secret management systems (e.g., HashiCorp Vault, AWS Secrets Manager).

3. **RBAC**: Generate user-specific kubeconfigs with minimal required permissions. Avoid creating admin tokens unless absolutely necessary.

4. **Network Policies**: Consider implementing NetworkPolicies to restrict access to the API server service.

5. **Token Expiry**: Set appropriate token expiry times when generating kubeconfigs:
   ```bash
   dctl generate-config --user=user --expiry=720h ...  # 30 days
   ```

## Upgrading

To upgrade to a new version:

```bash
helm repo update
helm upgrade dcontroller dcontroller/dcontroller
```

To change deployment mode (requires restart):

```bash
# Create TLS secret first (if switching to production)
kubectl create secret tls dcontroller-tls \
  --cert=apiserver.crt --key=apiserver.key -n dcontroller-system

# Upgrade
helm upgrade dcontroller dcontroller/dcontroller \
  --set apiServer.mode=production \
  --set apiServer.service.type=LoadBalancer
```

## Troubleshooting

### API Server Not Starting

Check logs:
```bash
kubectl logs -n dcontroller-system deployment/dcontroller-manager
```

Common issues:
- TLS secret not found (production mode): Ensure secret exists
- Port conflict: Check if port 8443 is available
- Authentication failures: Verify certificate/key pair is valid

### Cannot Access API Server

1. Check service:
   ```bash
   kubectl get svc dcontroller-apiserver -n dcontroller-system
   ```

2. Check pod status:
   ```bash
   kubectl get pods -n dcontroller-system
   ```

3. Test connectivity:
   ```bash
   # For ClusterIP
   kubectl -n dcontroller-system port-forward svc/dcontroller-apiserver 8443:8443

   # For LoadBalancer/NodePort
   curl -k https://<IP>:8443/api
   ```

### Authentication Failures

Verify token:
```bash
export KUBECONFIG=./your.config
dctl get-config --tls-cert-file=apiserver.crt
```

Common issues:
- Token expired: Generate new token
- Wrong certificate: Ensure certificate matches the one used by API server
- Namespace restrictions: Check if user has access to requested namespace

## Uninstallation

```bash
helm uninstall dcontroller
kubectl delete namespace dcontroller-system
```

## Support

For issues and questions:
- GitHub: https://github.com/l7mp/dcontroller/issues
- Documentation: https://github.com/l7mp/dcontroller/tree/master/doc
