package object

import (
	"encoding/base64"
)

// DecodeSecretData decodes base64-encoded data fields in a Kubernetes Secret object.
// This function modifies the object in-place, converting the base64-encoded strings
// in the "data" field to their decoded plaintext equivalents.
//
// This is useful when Secret objects enter the controller pipeline from watches,
// allowing pipeline expressions to access the decoded secret values directly
// without manual base64 decoding.
//
// If the object is not a Secret (kind != "Secret"), this function does nothing.
// If decoding fails for any field, the original base64 value is retained.
func DecodeSecretData(obj Object) Object {
	if obj == nil {
		return obj
	}

	// Check if this is a Secret object.
	if obj.GetKind() != "Secret" {
		return obj
	}

	// Get the data map.
	content := obj.UnstructuredContent()
	dataField, found := content["data"]
	if !found {
		return obj
	}

	dataMap, ok := dataField.(map[string]any)
	if !ok {
		return obj
	}

	// Decode each base64-encoded field.
	for key, value := range dataMap {
		if strValue, ok := value.(string); ok {
			decoded, err := base64.StdEncoding.DecodeString(strValue)
			if err != nil {
				// If decoding fails, keep the original value.
				// This handles cases where data is malformed or already decoded.
				continue
			}
			dataMap[key] = string(decoded)
		}
	}

	return obj
}
