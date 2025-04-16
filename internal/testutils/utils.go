package testutils

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var (
	TestNs = &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "testnamespace",
		},
	}

	TestNs2 = &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other",
		},
	}

	TestSvc = &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      "test-service",
				"namespace": "default",
			},
			"spec": map[string]any{
				"selector": map[string]any{
					"app": "example",
				},
				"ports": []any{
					map[string]any{
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
		Object: map[string]any{
			"apiVersion": "discovery.k8s.io/v1",
			"kind":       "EndpointSlice",
			"metadata": map[string]any{
				"name":      "test-endpointslice",
				"namespace": "default",
				"labels": map[string]any{
					"kubernetes.io/service-name": "test-service",
				},
			},
			"addressType": "IPv4",
			"ports": []any{
				map[string]any{
					"name":     "http",
					"port":     int64(80),
					"protocol": "TCP",
				},
			},
			"endpoints": []any{
				map[string]any{
					"addresses": []any{
						"192.0.2.1",
					},
					"conditions": map[string]any{
						"ready": true,
					},
					"hostname": "pod-1",
					"targetRef": map[string]any{
						"kind": "Pod",
						"name": "example-pod-1",
					},
				},
				map[string]any{
					"addresses": []any{
						"192.0.2.2",
					},
					"conditions": map[string]any{
						"ready": true,
					},
					"hostname": "pod-2",
					"targetRef": map[string]any{
						"kind": "Pod",
						"name": "example-pod-2",
					},
				},
			},
		},
	}

	TestConfigMap = &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "test-configmap",
				"namespace": "default",
			},
			"data": map[string]any{
				"key1": "value1",
				"key2": "value2",
			},
		},
	}

	TestDeployment = &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "test-deployment",
				"namespace": "default",
				"annotations": map[string]any{
					"dcontroller.io/related-configmap": "test-configmap",
				},
			},
			"spec": map[string]any{
				"replicas": int64(1),
				"selector": map[string]any{
					"matchLabels": map[string]any{
						"app": "test",
					},
				},
				"template": map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{
							"app": "test",
						},
					},
					"spec": map[string]any{
						"containers": []any{
							map[string]any{
								"name":  "test-container",
								"image": "nginx:latest",
							},
						},
					},
				},
			},
		},
	}

	TestPod = &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "/v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name":      "testpod",
				"namespace": "testnamespace",
			},
			"spec": map[string]any{
				"containers": []any{
					map[string]any{
						"name":  "nginx",
						"image": "nginx",
					},
					map[string]any{
						"name":  "pause",
						"image": "pause",
					},
				},
			},
		},
	}
)
