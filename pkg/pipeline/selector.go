package pipeline

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func MatchLabels(labelMap map[string]any, selectorMap map[string]any) (bool, error) {
	// Convert labelMap to labels.Set
	set := labels.Set{}
	for k, v := range labelMap {
		set[k] = fmt.Sprintf("%v", v)
	}

	// Create a LabelSelector from selectorMap
	labelSelector, err := convertToLabelSelector(selectorMap)
	if err != nil {
		return false, err
	}

	// Convert LabelSelector to Selector
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return false, err
	}

	// Perform the match
	return selector.Matches(set), nil
}

func convertToLabelSelector(selectorMap map[string]any) (*metav1.LabelSelector, error) {
	labelSelector := &metav1.LabelSelector{
		MatchLabels:      make(map[string]string),
		MatchExpressions: []metav1.LabelSelectorRequirement{},
	}

	matchLabels, hasMatchLabels := selectorMap["matchLabels"].(map[string]any)
	matchExpressions, hasMatchExpressions := selectorMap["matchExpressions"].([]any)

	if !hasMatchLabels && !hasMatchExpressions {
		// Treat the entire selectorMap as matchLabels
		for k, v := range selectorMap {
			labelSelector.MatchLabels[k] = fmt.Sprintf("%v", v)
		}
	} else {
		if hasMatchLabels {
			for k, v := range matchLabels {
				labelSelector.MatchLabels[k] = fmt.Sprintf("%v", v)
			}
		}

		if hasMatchExpressions {
			for _, expr := range matchExpressions {
				exprMap, ok := expr.(map[string]any)
				if !ok {
					continue
				}

				key, _ := exprMap["key"].(string)
				operator, _ := exprMap["operator"].(string)
				values, _ := exprMap["values"].([]any)

				switch operator {
				case "In", "NotIn", "Exists", "DoesNotExist":
					// this is OK
				default:
					return nil, fmt.Errorf("unknown label selector operator %s", operator)
				}

				requirement := metav1.LabelSelectorRequirement{
					Key:      key,
					Operator: metav1.LabelSelectorOperator(operator),
				}

				for _, v := range values {
					requirement.Values = append(requirement.Values, fmt.Sprintf("%v", v))
				}

				labelSelector.MatchExpressions = append(labelSelector.MatchExpressions, requirement)
			}
		}
	}

	return labelSelector, nil
}
