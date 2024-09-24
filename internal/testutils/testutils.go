package testutils

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

var (
	TestSvc = &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata": map[string]interface{}{
				"name":      "test-service",
				"namespace": "default",
			},
			"spec": map[string]interface{}{
				"selector": map[string]interface{}{
					"app": "example",
				},
				"ports": []interface{}{
					map[string]interface{}{
						"protocol":   "TCP",
						"port":       int64(80),
						"targetPort": int64(8080),
					},
				},
				"type": "ClusterIP",
			},
		},
	}

	TestEndpointSlice = &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "discovery.k8s.io/v1",
			"kind":       "EndpointSlice",
			"metadata": map[string]interface{}{
				"name":      "test-endpointslice",
				"namespace": "default",
				"labels": map[string]interface{}{
					"kubernetes.io/service-name": "test-service",
				},
			},
			"addressType": "IPv4",
			"ports": []interface{}{
				map[string]interface{}{
					"name":     "http",
					"port":     int64(80),
					"protocol": "TCP",
				},
			},
			"endpoints": []interface{}{
				map[string]interface{}{
					"addresses": []interface{}{
						"192.0.2.1",
					},
					"conditions": map[string]interface{}{
						"ready": true,
					},
					"hostname": "pod-1",
					"targetRef": map[string]interface{}{
						"kind": "Pod",
						"name": "example-pod-1",
					},
				},
				map[string]interface{}{
					"addresses": []interface{}{
						"192.0.2.2",
					},
					"conditions": map[string]interface{}{
						"ready": true,
					},
					"hostname": "pod-2",
					"targetRef": map[string]interface{}{
						"kind": "Pod",
						"name": "example-pod-2",
					},
				},
			},
		},
	}
)
