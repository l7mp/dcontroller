package object

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDecodeSecretData(t *testing.T) {
	tests := []struct {
		name     string
		input    *corev1.Secret
		expected map[string]string
	}{
		{
			name: "decode simple secret with multiple keys",
			input: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "testnamespace",
					Name:      "testsecret",
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{
					"username": []byte("admin"),
					"password": []byte("s3cr3t"),
					"config":   []byte(`{"key":"value"}`),
				},
			},
			expected: map[string]string{
				"username": "admin",
				"password": "s3cr3t",
				"config":   `{"key":"value"}`,
			},
		},
		{
			name: "decode secret with binary data",
			input: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "testnamespace",
					Name:      "binarysecret",
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{
					"binarykey": {0x00, 0x01, 0x02, 0xFF},
				},
			},
			expected: map[string]string{
				"binarykey": "\x00\x01\x02\xff",
			},
		},
		{
			name: "decode secret with empty data",
			input: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "testnamespace",
					Name:      "emptysecret",
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{},
			},
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert Secret to unstructured object.
			obj, err := NewViewObjectFromNativeObject("test", "Secret", tt.input)
			if err != nil {
				t.Fatalf("failed to create object: %v", err)
			}

			// Decode the secret data.
			DecodeSecretData(obj)

			// Verify decoded values.
			content := obj.UnstructuredContent()
			dataField, found := content["data"]
			if !found && len(tt.expected) > 0 {
				t.Fatal("data field not found in object")
			}

			if len(tt.expected) == 0 {
				return // Empty data, nothing to check.
			}

			dataMap, ok := dataField.(map[string]any)
			if !ok {
				t.Fatalf("data field is not a map: %T", dataField)
			}

			for key, expectedValue := range tt.expected {
				actualValue, found := dataMap[key]
				if !found {
					t.Errorf("key %q not found in decoded data", key)
					continue
				}

				actualStr, ok := actualValue.(string)
				if !ok {
					t.Errorf("value for key %q is not a string: %T", key, actualValue)
					continue
				}

				if actualStr != expectedValue {
					t.Errorf("key %q: got %q, want %q", key, actualStr, expectedValue)
				}
			}
		})
	}
}

func TestDecodeSecretData_NonSecret(t *testing.T) {
	// Create a non-Secret object (e.g., ConfigMap)
	cm := NewViewObject("test", "ConfigMap")
	SetName(cm, "default", "testconfigmap")
	SetContent(cm, map[string]any{
		"data": map[string]any{
			"key": "value",
		},
	})

	// Call DecodeSecretData on a non-Secret object.
	DecodeSecretData(cm)

	// Verify data is unchanged.
	content := cm.UnstructuredContent()
	dataField, found := content["data"]
	if !found {
		t.Fatal("data field not found")
	}

	dataMap, ok := dataField.(map[string]any)
	if !ok {
		t.Fatalf("data field is not a map: %T", dataField)
	}

	if value, ok := dataMap["key"]; !ok || value != "value" {
		t.Errorf("non-Secret data was modified: got %v", dataMap)
	}
}

func TestDecodeSecretData_NilObject(t *testing.T) {
	// Should not panic on nil object.
	DecodeSecretData(nil)
}
