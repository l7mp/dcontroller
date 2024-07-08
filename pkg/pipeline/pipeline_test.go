package pipeline

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	loglevel = -10
	logger   = zap.New(zap.UseFlagOptions(&zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}))
)

func TestPipeline(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pipeline")
}

var TestGw = map[string]any{
	"apiVersion": "gateway.networking.k8s.io/v1",
	"kind":       "Gateway",
	"metadata": map[string]any{
		"annotations": map[string]any{
			"kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"gateway.networking.k8s.io/v1\",\"kind\":\"Gateway\",\"metadata\":{\"annotations\":{},\"name\":\"udp-gateway\",\"namespace\":\"stunner\"},\"spec\":{\"gatewayClassName\":\"stunner-gatewayclass\",\"listeners\":[{\"name\":\"udp-listener\",\"port\":3478,\"protocol\":\"TURN-UDP\"}]}}",
		},
		"creationTimestamp": "2024-06-27T13:45:32Z",
		"generation":        int64(1),
		"name":              "udp-gateway",
		"namespace":         "stunner",
		"resourceVersion":   "38412796",
		"uid":               "3312bf0c-ccac-4998-bbd7-743522839dd2",
	},
	"spec": map[string]any{
		"gatewayClassName": "stunner-gatewayclass",
		"listeners": []any{
			map[string]any{
				"allowedRoutes": map[string]any{
					"namespaces": map[string]any{
						"from": "Same",
					},
				},
				"name":     "udp-listener",
				"port":     int64(3478),
				"protocol": "TURN-UDP",
			},
		},
	},
	"status": map[string]any{
		"addresses": []any{
			map[string]any{
				"type":  "IPAddress",
				"value": "35.232.39.111",
			},
		},
		"conditions": []any{
			map[string]any{
				"lastTransitionTime": "2024-07-01T12:46:03Z",
				"message":            "gateway accepted by controller stunner.l7mp.io/gateway-operator",
				"observedGeneration": int64(1),
				"reason":             "Accepted",
				"status":             "True",
				"type":               "Accepted",
			},
			map[string]any{
				"lastTransitionTime": "2024-07-08T05:36:35Z",
				"message":            "dataplane configuration successfully rendered",
				"observedGeneration": int64(1),
				"reason":             "Programmed",
				"status":             "True",
				"type":               "Programmed",
			},
		},
		"listeners": []any{
			map[string]any{
				"attachedRoutes": int64(1),
				"conditions": []any{
					map[string]any{
						"lastTransitionTime": "2024-07-08T05:36:35Z",
						"message":            "listener accepted",
						"observedGeneration": int64(1),
						"reason":             "Accepted",
						"status":             "True",
						"type":               "Accepted",
					},
					map[string]any{
						"lastTransitionTime": "2024-07-08T05:36:35Z",
						"message":            "listener protocol-port available",
						"observedGeneration": int64(1),
						"reason":             "NoConflicts",
						"status":             "False",
						"type":               "Conflicted",
					},
					map[string]any{
						"lastTransitionTime": "2024-07-08T05:36:35Z",
						"message":            "listener object references sucessfully resolved",
						"observedGeneration": int64(1),
						"reason":             "ResolvedRefs",
						"status":             "True",
						"type":               "ResolvedRefs",
					},
				},
				"name": "udp-listener",
				"supportedKinds": []any{
					map[string]any{
						"group": "gateway.networking.k8s.io",
						"kind":  "UDPRoute",
					},
					map[string]any{
						"group": "stunner.l7mp.io",
						"kind":  "UDPRoute",
					},
				},
			},
		},
	},
}
